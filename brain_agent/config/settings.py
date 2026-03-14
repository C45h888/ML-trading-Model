"""
Settings Management - Single source of truth for Brain Agent
Uses Pydantic v2 BaseSettings for type-safe configuration
Auto-merges .env + YAML + defaults
"""
from typing import Optional, Dict, List
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml


# Paths
CONFIG_DIR = Path(__file__).parent
PROJECT_ROOT = CONFIG_DIR.parent
DEFAULT_YAML = CONFIG_DIR / "data_sources.yaml"


class HyperliquidSettings(BaseSettings):
    """Hyperliquid-specific configuration"""
    ws_url: str = "wss://api.hyperliquid.xyz/ws"
    testnet: bool = False
    coins: List[str] = ["BTC", "ETH", "SOL"]
    api_url: Optional[str] = None
    
    @model_validator(mode='after')
    def set_api_url(self):
        """Set API URL based on testnet flag"""
        if self.api_url is None:
            self.api_url = "https://api.hyperliquid.xyz" if not self.testnet else "https://api.hyperliquid-testnet.xyz"
        return self


class SierraDTCSettings(BaseSettings):
    """Sierra Chart DTC configuration"""
    host: str = "127.0.0.1"
    port: int = 11095
    history_port: int = 11096
    username: str = ""
    password: str = ""


class TradingSettings(BaseSettings):
    """Trading configuration"""
    default_symbol: str = "BTC"
    default_timeframe: str = "1m"  # 1m, 5m, 15m, 1h, 4h, 1d
    initial_capital: float = 100000.0


class ExecutionSettings(BaseSettings):
    """Execution mode settings"""
    mode: str = "backtest"  # backtest, paper, live


class RedisSettings(BaseSettings):
    """Redis connection settings"""
    host: str = "localhost"
    port: int = 6379
    password: str = ""
    db: int = 0
    url: Optional[str] = None
    
    @model_validator(mode='after')
    def build_url(self):
        """Build Redis URL from components"""
        if self.url is None:
            if self.password:
                self.url = f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
            else:
                self.url = f"redis://{self.host}:{self.port}/{self.db}"
        return self


class Settings(BaseSettings):
    """
    Main Settings class - single source of truth for the entire agent.
    
    Loads configuration from:
    1. Environment variables (highest priority)
    2. .env file
    3. data_sources.yaml
    4. Default values (lowest priority)
    
    Usage:
        from brain_agent.config.settings import settings
        print(settings.data_source)
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Top-level settings
    data_source: str = "hyperliquid"
    log_level: str = "INFO"
    
    # Nested settings
    hyperliquid: HyperliquidSettings = Field(default_factory=HyperliquidSettings)
    sierra_dtc: SierraDTCSettings = Field(default_factory=SierraDTCSettings)
    trading: TradingSettings = Field(default_factory=TradingSettings)
    execution: ExecutionSettings = Field(default_factory=ExecutionSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    
    # Additional settings
    symbols: List[str] = ["BTC", "ETH", "SOL"]
    
    @model_validator(mode='after')
    def validate_config(self):
        """Cross-field validation"""
        # Validate data_source
        valid_sources = ["hyperliquid", "sierra_dtc"]
        if self.data_source not in valid_sources:
            raise ValueError(f"data_source must be one of {valid_sources}, got {self.data_source}")
        
        # Validate execution mode
        valid_modes = ["backtest", "paper", "live"]
        if self.execution.mode not in valid_modes:
            raise ValueError(f"mode must be one of {valid_modes}, got {self.execution.mode}")
        
        # Validate log level
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        
        return self
    
    @classmethod
    def load_yaml(cls, yaml_path: Path = DEFAULT_YAML) -> Dict:
        """Load base configuration from YAML file"""
        if yaml_path.exists():
            with open(yaml_path, 'r') as f:
                return yaml.safe_load(f) or {}
        return {}
    
    @classmethod
    def from_yaml_with_env(cls, yaml_path: Path = DEFAULT_YAML) -> "Settings":
        """
        Factory method to create Settings from YAML + environment.
        Environment variables override YAML values.
        """
        yaml_config = cls.load_yaml(yaml_path)
        
        # Extract nested configs
        hyperliquid_data = yaml_config.get("hyperliquid", {})
        sierra_dtc_data = yaml_config.get("sierra_dtc", {})
        trading_data = yaml_config.get("trading", {})
        execution_data = yaml_config.get("execution", {})
        
        # Build kwargs for Settings
        kwargs = {
            "data_source": yaml_config.get("default_source", "hyperliquid"),
            "symbols": yaml_config.get("symbols", ["BTC", "ETH", "SOL"]),
            "hyperliquid": hyperliquid_data,
            "sierra_dtc": sierra_dtc_data,
            "trading": trading_data,
            "execution": execution_data,
        }
        
        return cls(**kwargs)


# Global singleton instance
settings = Settings.from_yaml_with_env()


def get_settings() -> Settings:
    """Get the global settings instance"""
    return settings


def reload_settings() -> Settings:
    """Reload settings from disk (useful for config hot-reload)"""
    global settings
    settings = Settings.from_yaml_with_env()
    return settings
