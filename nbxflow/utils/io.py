"""I/O utilities for nbxflow."""

import json
import os
import yaml
from typing import Dict, Any, Optional, Union
from pathlib import Path

from .logging import get_logger

logger = get_logger(__name__)

def write_json(data: Dict[str, Any], path: Union[str, Path], indent: int = 2) -> None:
    """Write data to JSON file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w') as f:
        json.dump(data, f, indent=indent)
    
    logger.debug(f"Written JSON to {path}")

def read_json(path: Union[str, Path]) -> Dict[str, Any]:
    """Read data from JSON file."""
    with open(path, 'r') as f:
        return json.load(f)

def write_yaml(data: Dict[str, Any], path: Union[str, Path]) -> None:
    """Write data to YAML file."""
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML required for YAML support. Install with: pip install pyyaml")
    
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False, indent=2)
    
    logger.debug(f"Written YAML to {path}")

def read_yaml(path: Union[str, Path]) -> Dict[str, Any]:
    """Read data from YAML file."""
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML required for YAML support. Install with: pip install pyyaml")
    
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def ensure_directory(path: Union[str, Path]) -> Path:
    """Ensure directory exists and return Path object."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path

def get_project_root() -> Path:
    """Get the project root directory."""
    current = Path.cwd()
    
    # Look for common project indicators
    indicators = ['.git', 'pyproject.toml', 'setup.py', '.nbxflow']
    
    while current != current.parent:
        for indicator in indicators:
            if (current / indicator).exists():
                return current
        current = current.parent
    
    # If no indicators found, return current working directory
    return Path.cwd()

def find_flow_files(directory: Union[str, Path] = None, pattern: str = "*_flow_*.json") -> list:
    """Find FlowSpec files in directory."""
    directory = Path(directory) if directory else Path.cwd()
    return list(directory.glob(pattern))