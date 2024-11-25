"""Entities validation workflow task"""

from collections.abc import Sequence

from cmem.cmempy.workspace.projects.resources.resource import get_resource_response
from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    ExecutionReport,
    UserContext,
)
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.utils import (
    setup_cmempy_user_access,
    split_task_id,
)
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from cmem_plugin_validation.validate_entities import state

DEFAULT_FAIL_ON_VIOLATION = False


def get_task_metadata(project: str, task: str, context: UserContext) -> dict:
    """Get metadata information of a task"""
    setup_cmempy_user_access(context=context)
    return dict(get_task(project=project, task=task))


@Plugin(
    label="Validate Entity",
    plugin_id="cmem_plugin_validation-validate-ValidateEntity",
    description="Use JSON schema to validate entities",
    documentation="Sai",
    parameters=[
        PluginParameter(
            name="json_dataset",
            label="JSON Dataset",
            description="This dataset  holds the resources you want to validate.",
            param_type=DatasetParameterType(dataset_type="json"),
        ),
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

    def __init__(self, json_dataset: str, json_schema_dataset: str, fail_on_violations: bool):
        self.json_dataset = json_dataset
        self.json_schema_dataset = json_schema_dataset
        self.fail_on_violations = fail_on_violations
        self._state = state.State()

    def execute(
        self,
        inputs: Sequence[Entities],  # noqa: ARG002
        context: ExecutionContext,
    ) -> Entities | None:
        """Run the workflow operator."""
        _ = context

        json_data_set = self._get_json_dataset_content(context, self.json_dataset)
        json_data_set_schema = self._get_json_dataset_content(context, self.json_schema_dataset)
        if isinstance(json_data_set, list):
            for _ in json_data_set:
                self._validate_json(_, json_data_set_schema)  # type: ignore[arg-type]
        else:
            self._validate_json(json_data_set, json_data_set_schema)  # type: ignore[arg-type]
        _state = self._state
        summary: list[tuple[str, str]] = [
            (str(_), message) for _, message in enumerate(_state.violations_messages)
        ]
        validation_message = None
        if _state.violations:
            validation_message = f"Found {_state.violations} violations in {_state.total} entities"
        context.report.update(
            ExecutionReport(
                entity_count=_state.total,
                operation="read",
                operation_desc=" entities validated",
                summary=summary,
                error=validation_message if self.fail_on_violations else None,
                warnings=[validation_message]
                if not self.fail_on_violations and _state.violations
                else [],
            )
        )
        return None

    def _validate_json(self, json: dict, schema: dict) -> None:
        """Validate JSON"""
        try:
            self._state.increment_total()
            validate(instance=json, schema=schema)
        except ValidationError as e:
            self._state.add_violations_message(e.message)

    @staticmethod
    def _get_json_dataset_content(context: ExecutionContext, dataset: str) -> dict | list[dict]:
        """Get json dataset content"""
        dataset_id = f"{context.task.project_id()}:{dataset}"
        project_id, task_id = split_task_id(dataset_id)
        task_meta_data = get_task_metadata(project_id, task_id, context.user)
        resource_name = str(task_meta_data["data"]["parameters"]["file"]["value"])
        response = get_resource_response(project_id, resource_name)
        return response.json()  # type: ignore[no-any-return]
