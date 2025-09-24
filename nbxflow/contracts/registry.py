import os
import json
import hashlib
from typing import Dict, Any, Optional, List
from datetime import datetime

from ..config import settings
from ..utils.logging import get_logger

logger = get_logger(__name__)

class ContractRegistry:
    """Registry for managing data contract versions."""
    
    def __init__(self, base_dir: Optional[str] = None):
        self.base_dir = base_dir or settings.contracts_dir
        self._ensure_directory()
    
    def _ensure_directory(self):
        """Ensure the contracts directory exists."""
        os.makedirs(self.base_dir, exist_ok=True)
        
        # Create index file if it doesn't exist
        index_path = os.path.join(self.base_dir, "index.json")
        if not os.path.exists(index_path):
            self._save_index({})
    
    def _get_index_path(self) -> str:
        """Get path to the index file."""
        return os.path.join(self.base_dir, "index.json")
    
    def _load_index(self) -> Dict[str, Any]:
        """Load the contract index."""
        try:
            with open(self._get_index_path(), 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    
    def _save_index(self, index: Dict[str, Any]):
        """Save the contract index."""
        with open(self._get_index_path(), 'w') as f:
            json.dump(index, f, indent=2)
    
    def _get_contract_path(self, suite_name: str, version: str) -> str:
        """Get path for a specific contract version."""
        return os.path.join(self.base_dir, f"{suite_name}_v{version}.json")
    
    def _calculate_hash(self, contract: Dict[str, Any]) -> str:
        """Calculate hash of contract content."""
        # Create a stable string representation
        content = json.dumps(contract.get("expectations", []), sort_keys=True)
        return hashlib.md5(content.encode()).hexdigest()[:8]
    
    def list_contracts(self) -> List[str]:
        """List all contract names."""
        index = self._load_index()
        return list(index.keys())
    
    def list_versions(self, suite_name: str) -> List[str]:
        """List all versions of a contract."""
        index = self._load_index()
        contract_info = index.get(suite_name, {})
        return list(contract_info.get("versions", {}).keys())
    
    def get_latest_version(self, suite_name: str) -> Optional[str]:
        """Get the latest version number for a contract."""
        versions = self.list_versions(suite_name)
        if not versions:
            return None
        
        # Sort versions numerically
        try:
            numeric_versions = [int(v) for v in versions]
            return str(max(numeric_versions))
        except ValueError:
            # Fallback to string sorting if not all numeric
            return sorted(versions)[-1]
    
    def save_contract(self, suite_name: str, contract: Dict[str, Any], 
                     version: Optional[str] = None, auto_increment: bool = True) -> str:
        """
        Save a contract to the registry.
        
        Args:
            suite_name: Name of the contract suite
            contract: Contract dictionary
            version: Specific version (if None, will auto-increment)
            auto_increment: Whether to auto-increment version if not specified
        
        Returns:
            The version string that was saved
        """
        index = self._load_index()
        
        # Initialize contract in index if new
        if suite_name not in index:
            index[suite_name] = {
                "versions": {},
                "latest": None,
                "created_at": datetime.utcnow().isoformat()
            }
        
        contract_info = index[suite_name]
        
        # Determine version
        if version is None and auto_increment:
            latest = self.get_latest_version(suite_name)
            if latest is None:
                version = "1"
            else:
                try:
                    version = str(int(latest) + 1)
                except ValueError:
                    version = "1"
        elif version is None:
            version = "1"
        
        # Add metadata to contract
        contract_with_meta = {
            **contract,
            "metadata": {
                "suite_name": suite_name,
                "version": version,
                "created_at": datetime.utcnow().isoformat(),
                "hash": self._calculate_hash(contract)
            }
        }
        
        # Save contract file
        contract_path = self._get_contract_path(suite_name, version)
        with open(contract_path, 'w') as f:
            json.dump(contract_with_meta, f, indent=2)
        
        # Update index
        contract_info["versions"][version] = {
            "created_at": datetime.utcnow().isoformat(),
            "hash": self._calculate_hash(contract),
            "path": contract_path,
            "expectations_count": len(contract.get("expectations", []))
        }
        contract_info["latest"] = version
        
        self._save_index(index)
        
        logger.info(f"Saved contract {suite_name} version {version}")
        return version
    
    def load_contract(self, suite_name: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Load a contract from the registry.
        
        Args:
            suite_name: Name of the contract suite
            version: Version to load (if None, loads latest)
        
        Returns:
            Contract dictionary or None if not found
        """
        if version is None:
            version = self.get_latest_version(suite_name)
        
        if version is None:
            logger.warning(f"No versions found for contract: {suite_name}")
            return None
        
        contract_path = self._get_contract_path(suite_name, version)
        
        try:
            with open(contract_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Contract file not found: {contract_path}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in contract file {contract_path}: {e}")
            return None
    
    def compare_contracts(self, suite_name: str, version1: str, version2: str) -> Dict[str, Any]:
        """
        Compare two versions of a contract.
        
        Returns:
            Dictionary with comparison results
        """
        contract1 = self.load_contract(suite_name, version1)
        contract2 = self.load_contract(suite_name, version2)
        
        if not contract1 or not contract2:
            return {"error": "One or both contracts not found"}
        
        exp1 = contract1.get("expectations", [])
        exp2 = contract2.get("expectations", [])
        
        # Simple comparison based on expectation count and types
        exp_types1 = {exp.get("expectation_type") for exp in exp1}
        exp_types2 = {exp.get("expectation_type") for exp in exp2}
        
        added_types = exp_types2 - exp_types1
        removed_types = exp_types1 - exp_types2
        
        # Check for breaking changes (removed expectations or stricter constraints)
        breaking_changes = bool(removed_types)
        
        return {
            "version1": version1,
            "version2": version2,
            "expectations_count": {
                "v1": len(exp1),
                "v2": len(exp2),
                "diff": len(exp2) - len(exp1)
            },
            "expectation_types": {
                "added": list(added_types),
                "removed": list(removed_types),
                "common": list(exp_types1 & exp_types2)
            },
            "breaking_changes": breaking_changes,
            "hash_changed": (
                contract1.get("metadata", {}).get("hash") != 
                contract2.get("metadata", {}).get("hash")
            )
        }
    
    def get_contract_info(self, suite_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a contract."""
        index = self._load_index()
        return index.get(suite_name)
    
    def delete_contract(self, suite_name: str, version: Optional[str] = None) -> bool:
        """
        Delete a contract version or entire contract.
        
        Args:
            suite_name: Name of the contract suite
            version: Version to delete (if None, deletes all versions)
        
        Returns:
            True if deletion was successful
        """
        index = self._load_index()
        
        if suite_name not in index:
            logger.warning(f"Contract {suite_name} not found")
            return False
        
        if version is None:
            # Delete all versions
            versions = self.list_versions(suite_name)
            for v in versions:
                contract_path = self._get_contract_path(suite_name, v)
                try:
                    os.remove(contract_path)
                except FileNotFoundError:
                    pass
            
            # Remove from index
            del index[suite_name]
            logger.info(f"Deleted all versions of contract {suite_name}")
        else:
            # Delete specific version
            contract_path = self._get_contract_path(suite_name, version)
            try:
                os.remove(contract_path)
                
                # Update index
                if version in index[suite_name]["versions"]:
                    del index[suite_name]["versions"][version]
                    
                    # Update latest version
                    remaining_versions = list(index[suite_name]["versions"].keys())
                    if remaining_versions:
                        try:
                            numeric_versions = [int(v) for v in remaining_versions]
                            index[suite_name]["latest"] = str(max(numeric_versions))
                        except ValueError:
                            index[suite_name]["latest"] = sorted(remaining_versions)[-1]
                    else:
                        index[suite_name]["latest"] = None
                
                logger.info(f"Deleted contract {suite_name} version {version}")
            except FileNotFoundError:
                logger.warning(f"Contract file not found: {contract_path}")
                return False
        
        self._save_index(index)
        return True