from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import yaml
from src.clusterbeacon.constants import REQUIRED_CONFIG_KEYS
from dataclasses import dataclass, field


@dataclass
class GenericConfig:
    data: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Wraps any dict inside a dataclass."""
        return cls(data)

class ConfigError(Exception):
    """Configuration-related errors."""


class ConfigLoader:
    """
    Load and validate a ClusterBeacon configuration file.

    Usage
    -----
    loader = ConfigLoader("config.yaml")
    if not loader.status:
        print(loader.errors)
    cfg = loader.config
    """

    def __init__(
        self,
        config_path: str | Path,
    ) -> None:
        self.config_path = Path(config_path)
        self.errors: List[str] = []
        self.config: dataclass = GenericConfig({})
        self.status: bool = False

        try:
            cfg = self.load_config(self.config_path)
            self.validate_config(cfg, REQUIRED_CONFIG_KEYS)
            self.config = cfg
            self.status = True
        except ConfigError as e:
            self.errors.append(str(e))
            self.status = False


    @staticmethod
    def load_config(config_path: str | Path) -> Dict[str, Any]:
        """
        Load a ClusterBeacon configuration file (YAML preferred, JSON supported).
        """
        path = Path(config_path)
        if not path.exists():
            raise ConfigError(f"Configuration file not found: {path}")

        try:
            text = path.read_text(encoding="utf-8")
            suffix = path.suffix.lower()

            if suffix in (".yaml", ".yml"):
                cfg = yaml.safe_load(text) or {}
            elif suffix == ".json":
                cfg = json.loads(text)
            else:
                # Try YAML first, then JSON, if unknown/absent extension
                try:
                    cfg = yaml.safe_load(text) or {}
                except yaml.YAMLError:
                    cfg = json.loads(text)

            if not isinstance(cfg, dict):
                raise ConfigError("Top-level config must be a mapping/object.")
            
            return GenericConfig(cfg)

        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise ConfigError(f"Error parsing config file {path}: {e}") from e

    @staticmethod
    def validate_config(config: Dict[str, Any], required_keys: List[str]) -> None:
        """
        Validate required keys and basic types.
        """
        for key in required_keys:
            if key not in config:
                raise ConfigError(f"Missing required config key: {key}")

        # Example validations (customize as needed)
        if "allele_threshold" in config:
            v = config["allele_threshold"]
            if not isinstance(v, int) or v <= 0:
                raise ConfigError("allele_threshold must be a positive integer")

        if "output_dir" in config:
            if not isinstance(config["output_dir"], str) or not config["output_dir"]:
                raise ConfigError("output_dir must be a non-empty string path")

    @classmethod
    def load_and_validate(
        cls,
        config_path: str | Path,
        required_keys: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        One-shot helper: returns a validated config or raises ConfigError.
        """
        cfg = cls.load_config(config_path)
        cls.validate_config(cfg, (required_keys or ["allele_threshold", "output_dir"]))
        return cfg