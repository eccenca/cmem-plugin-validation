"""Entities validation workflow task"""

from collections.abc import Sequence

from cmem_plugin_base.dataintegration.context import ExecutionContext
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin

DEFAULT_FAIL_ON_VIOLATION = False


@Plugin(
    label="Validate Entity",
    plugin_id="cmem_plugin_validation-validate-ValidateEntity",
    description="Use JSON schema to validate entities",
    documentation="Sai",
    parameters=[
        PluginParameter(
            name="json_schema_dataset",
            label="JSON Schema Dataset",
            description="This dataset  holds the resources you want to validate.",
            param_type=DatasetParameterType(dataset_type="json"),
        ),
        PluginParameter(
            name="fail_on_violations",
            label="Fail workflow on violations",
            default_value=DEFAULT_FAIL_ON_VIOLATION,
        ),
    ],
)
class ValidateEntity(WorkflowPlugin):
    """Validate entities against a JSON schema"""

    def __init__(self, json_schema_dataset: str, fail_on_violations: bool):
        self.json_schema_dataset = json_schema_dataset
        self.fail_on_violations = fail_on_violations

    def execute(
        self,
        inputs: Sequence[Entities],  # noqa: ARG002
        context: ExecutionContext,
    ) -> Entities | None:
        """Run the workflow operator."""
        _ = context
        return None
