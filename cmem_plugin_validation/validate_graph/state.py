"""Graph validation process state"""

from cmem_client.client import Client


class State:
    """State of a validation process"""

    client: Client
    id_: str
    data: dict
    status: str
    completed: int
    total: int
    with_violations: int
    violations: int

    def __init__(self, client: Client, id_: str):
        self.client = client
        self.id_ = id_
        self.refresh()

    def refresh(self) -> None:
        """Refresh state of validation process"""
        aggregation = self.client.validations.get_aggregation(batch_id=self.id_)
        self.data = aggregation.model_dump(by_alias=True, exclude_none=True)
        self.status = aggregation.state
        self.completed = aggregation.resource_processed_count
        self.total = aggregation.resource_count
        self.with_violations = aggregation.resources_with_violations_count
        self.violations = aggregation.violations_count
