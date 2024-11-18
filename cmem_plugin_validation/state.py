"""Graph validation process state"""

from cmem.cmempy.dp.shacl import validation


class State:
    """State of a validation process"""

    id_: str
    data: dict
    status: str
    completed: int
    total: int
    with_violations: int

    def __init__(self, id_: str):
        self.id_ = id_
        self.refresh()

    def refresh(self) -> None:
        """Refresh state of validation process"""
        self.data = validation.get_aggregation(batch_id=self.id_)
        self.status = self.data.get("state", "UNKNOWN")
        self.completed = self.data.get("resourceProcessedCount", 0)
        self.total = self.data.get("resourceCount", 0)
        self.with_violations = self.data.get("resourcesWithViolationsCount", 0)
        self.violations = self.data.get("violationsCount", 0)
