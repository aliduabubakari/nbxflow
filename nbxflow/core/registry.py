from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
import json
from ..utils.time import now_iso_zulu

@dataclass
class TaskSpec:
    """Specification for a task in the flow."""
    name: str
    component_type: str
    inputs: List[Dict[str, Any]] = field(default_factory=list)
    outputs: List[Dict[str, Any]] = field(default_factory=list)
    contracts: List[Dict[str, Any]] = field(default_factory=list)
    tags: Dict[str, Any] = field(default_factory=dict)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    status: Optional[str] = None  # SUCCESS, FAILED, RUNNING
    run_id: Optional[str] = None
    parent: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "component_type": self.component_type,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "contracts": self.contracts,
            "tags": self.tags,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "run_id": self.run_id,
            "parent": self.parent
        }

@dataclass
class FlowRegistry:
    """Registry for tracking flow execution."""
    name: str
    run_id: str
    tasks: List[TaskSpec] = field(default_factory=list)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    status: str = "RUNNING"
    _task_stack: List[str] = field(default_factory=list, init=False)

    def add_task(self, task: TaskSpec) -> None:
        """Add a task to the registry."""
        self.tasks.append(task)

    def get_task(self, name: str, run_id: Optional[str] = None) -> Optional[TaskSpec]:
        """Get a task by name and optionally run_id."""
        for task in reversed(self.tasks):  # Get most recent
            if task.name == name:
                if run_id is None or task.run_id == run_id:
                    return task
        return None

    def update_task_status(self, name: str, run_id: str, status: str, 
                          inputs: Optional[List[Dict]] = None, 
                          outputs: Optional[List[Dict]] = None,
                          contracts: Optional[List[Dict]] = None) -> None:
        """Update task status and I/O."""
        task = self.get_task(name, run_id)
        if task:
            task.status = status
            task.finished_at = now_iso_zulu()
            if inputs is not None:
                task.inputs = inputs
            if outputs is not None:
                task.outputs = outputs
            if contracts is not None:
                task.contracts = contracts

    def derive_edges(self) -> List[Tuple[str, str]]:
        """Derive edges between tasks based on dataset dependencies."""
        edges = []
        task_names = {task.name for task in self.tasks}
        
        for i, task_a in enumerate(self.tasks):
            for j, task_b in enumerate(self.tasks):
                if i >= j:  # Only check forward dependencies
                    continue
                
                # Check if any output of task_a matches input of task_b
                for output in task_a.outputs:
                    for input_ds in task_b.inputs:
                        if (output.get("namespace") == input_ds.get("namespace") and 
                            output.get("name") == input_ds.get("name")):
                            edges.append((task_a.name, task_b.name))
        
        return list(set(edges))  # Remove duplicates

    def get_parallelizable_tasks(self) -> Dict[str, bool]:
        """Determine which tasks can potentially run in parallel."""
        # Heuristic based on component types
        parallelizable_types = {
            "Transformer", "Enricher", "Reconciliator", 
            "QualityCheck", "Splitter"
        }
        
        return {
            task.name: task.component_type in parallelizable_types 
            for task in self.tasks
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "flow": self.name,
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "tasks": [task.to_dict() for task in self.tasks],
            "edges": self.derive_edges(),
            "parallelizable": self.get_parallelizable_tasks()
        }

    def to_json(self, path: str) -> None:
        """Save registry to JSON file."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FlowRegistry":
        """Load registry from dictionary."""
        registry = cls(
            name=data["flow"],
            run_id=data["run_id"],
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            status=data.get("status", "COMPLETED")
        )
        
        for task_data in data.get("tasks", []):
            task = TaskSpec(
                name=task_data["name"],
                component_type=task_data["component_type"],
                inputs=task_data.get("inputs", []),
                outputs=task_data.get("outputs", []),
                contracts=task_data.get("contracts", []),
                tags=task_data.get("tags", {}),
                started_at=task_data.get("started_at"),
                finished_at=task_data.get("finished_at"),
                status=task_data.get("status"),
                run_id=task_data.get("run_id"),
                parent=task_data.get("parent")
            )
            registry.add_task(task)
        
        return registry

    @classmethod
    def from_json(cls, path: str) -> "FlowRegistry":
        """Load registry from JSON file."""
        with open(path, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)