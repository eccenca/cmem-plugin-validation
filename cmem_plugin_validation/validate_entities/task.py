"""Entities validation workflow task"""

import io
import json
from collections import OrderedDict
from collections.abc import Generator, Sequence
from types import SimpleNamespace
from typing import Any

from cmem.cmempy.workspace.projects.resources.resource import get_resource_response
from cmem.cmempy.workspace.tasks import get_task
from cmem_plugin_base.dataintegration.context import (
    ExecutionContext,
    ExecutionReport,
    UserContext,
)
from cmem_plugin_base.dataintegration.description import Icon, Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.dataset import DatasetParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import (
    FixedNumberOfInputs,
    FlexibleNumberOfInputs,
    UnknownSchemaPort,
)
from cmem_plugin_base.dataintegration.utils import (
    setup_cmempy_user_access,
    split_task_id,
    write_to_dataset,
)
from cmem_plugin_base.dataintegration.utils.entity_builder import build_entities_from_data
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from cmem_plugin_validation.validate_entities import state

DOCUMENTATION = """[JSON Schema](https://json-schema.org/) specifies a JSON-based format to
define the structure of JSON data for validation, documentation, and interaction control.
It provides a contract for the JSON data required by a given application.

This workflow task can validate incoming entities or a stand-alone JSON dataset by using a
JSON Schema specification.

The used JSON Schema needs to be provided as a JSON Dataset in the project.

Validated data objects can be send to an output port, to further process them in the workflow,
or saved in a JSON dataset in the project.

The task can either fail instantly if there is a data violation, or just provide warning in the
workflow report and allow to run follow-up tasks based on the data which was validated.
"""

DEFAULT_FAIL_ON_VIOLATION = False


def get_task_metadata(project: str, task: str, context: UserContext) -> dict:
    """Get metadata information of a task"""
    setup_cmempy_user_access(context=context)
    return dict(get_task(project=project, task=task))


SOURCE = SimpleNamespace()
SOURCE.entities = "entities"
SOURCE.file = "dataset"
SOURCE.options = OrderedDict(
    {
        SOURCE.entities: f"{SOURCE.entities}: "
        "Validate entities received from the input port in the workflow.",
        SOURCE.file: f"{SOURCE.file}: "
        "Validate a JSON Dataset from a project (see advanced options).",
    }
)

TARGET = SimpleNamespace()
TARGET.entities = "entities"
TARGET.dataset = "dataset"
TARGET.options = OrderedDict(
    {
        TARGET.dataset: f"{TARGET.dataset}: "
        "Valid JSON objects will be is saved in a JSON dataset (see advanced options).",
        TARGET.entities: f"{TARGET.entities}: "
        "Valid JSON objects will be send as entities to the output port.",
    }
)


@Plugin(
    label="Validate Entities",
    plugin_id="cmem_plugin_validation-validate-ValidateEntities",
    icon=Icon(file_name="icon.svg", package=__package__),
    description="Use a JSON schema to validate entities or a JSON dataset.",
    documentation=DOCUMENTATION,
    parameters=[
        PluginParameter(
            name="source_mode",
            label="Source / Input Mode",
            description="",
            param_type=ChoiceParameterType(SOURCE.options),
            default_value=SOURCE.entities,
        ),
        PluginParameter(
            name="target_mode",
            label="Target / Output Mode",
            description="",
            param_type=ChoiceParameterType(TARGET.options),
            default_value=TARGET.entities,
        ),
        PluginParameter(
            name="source_dataset",
            label="Source JSON Dataset",
            description="This dataset holds the resources you want to validate.",
            param_type=DatasetParameterType(dataset_type="json"),
            advanced=True,
            default_value="",
        ),
        PluginParameter(
            name="target_dataset",
            label="Target JSON Dataset",
            description="This dataset will be used to store the valid JSON objects"
            " after validation.",
            param_type=DatasetParameterType(dataset_type="json"),
            default_value="",
            advanced=True,
        ),
        PluginParameter(
            name="json_schema_dataset",
            label="JSON Schema Dataset",
            description="This dataset holds the JSON schema to use for validation.",
            param_type=DatasetParameterType(dataset_type="json"),
        ),
        PluginParameter(
            name="fail_on_violations",
            label="Fail on violations",
            description="If enabled, the task will fail on the first data violation.",
            default_value=DEFAULT_FAIL_ON_VIOLATION,
        ),
    ],
)
class ValidateEntity(WorkflowPlugin):
    """Validate entities against a JSON schema"""

    source_mode: str
    target_mode: str
    source_dataset: str
    target_dataset: str

    inputs: Sequence[Entities]
    execution_context: ExecutionContext

    def __init__(  #  noqa: PLR0913
        self,
        source_mode: str,
        target_mode: str,
        json_schema_dataset: str,
        fail_on_violations: bool,
        source_dataset: str = "",
        target_dataset: str = "",
    ):
        self.source_mode = source_mode
        self.target_mode = target_mode
        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.json_schema_dataset = json_schema_dataset
        self.fail_on_violations = fail_on_violations
        self._state = state.State()
        self._validate_config()
        self._set_ports()

    def _raise_error(self, message: str) -> None:
        """Send a report and raise an error"""
        raise ValueError(message)

    def _validate_config(self) -> None:
        """Raise value errors on bad configurations"""
        if self.source_mode == SOURCE.file and self.source_dataset == "":
            self._raise_error(
                f"When using the source mode '{SOURCE.file}', "
                "you need to select a Source JSON Dataset."
            )
        if self.source_mode == SOURCE.entities and self.source_dataset != "":
            self._raise_error(
                f"When using the source mode '{SOURCE.entities}', "
                "you don't need to select a Source JSON Dataset."
            )
        if (
            self.source_mode == SOURCE.entities
            and hasattr(self, "execution_context")
            and not self.inputs
        ):
            self._raise_error(
                f"When using the source mode '{SOURCE.entities}', "
                "you need to pass entities to input port."
            )

        if self.target_mode == TARGET.dataset and self.target_dataset == "":
            self._raise_error(
                f"When using the target mode '{TARGET.dataset}', "
                "you need to select a Target JSON dataset."
            )
        if self.target_mode == SOURCE.entities and self.target_dataset != "":
            self._raise_error(
                f"When using the source mode '{TARGET.entities}', "
                "you don't need to select a Target JSON Dataset."
            )

    def _set_ports(self) -> None:
        """Define input/output ports based on the configuration"""
        match self.source_mode:
            case SOURCE.file:
                # no input port
                self.input_ports = FixedNumberOfInputs([])
            case SOURCE.entities:
                self.input_ports = FlexibleNumberOfInputs()
            case _:
                raise ValueError(f"Unknown source mode: {self.source_mode}")
        match self.target_mode:
            case TARGET.entities:
                # output port with flexible schema
                self.output_port = UnknownSchemaPort()
            case TARGET.dataset:
                # not output port
                self.output_port = None
            case _:
                raise ValueError(f"Unknown target mode: {self.target_mode}")

    def execute(
        self,
        inputs: Sequence[Entities],
        context: ExecutionContext,
    ) -> Entities | None:
        """Run the workflow operator."""
        self.execution_context = context
        self.inputs = inputs
        self._validate_config()
        json_data_set_schema = self._get_json_dataset_content(context, self.json_schema_dataset)
        valid_json_objects = []
        if self.source_mode == SOURCE.entities:
            valid_json_objects += [
                _j
                for _j in self._convert_entities_to_json(inputs, {}, "")
                if _j and self._validate_json(_j, json_data_set_schema)  # type: ignore[arg-type]
            ]

        else:
            json_data_set = self._get_json_dataset_content(context, self.source_dataset)
            if isinstance(json_data_set, list):
                valid_json_objects += [
                    _
                    for _ in json_data_set
                    if self._validate_json(_, json_data_set_schema)  # type: ignore[arg-type]
                ]
            elif self._validate_json(json_data_set, json_data_set_schema):  # type: ignore[arg-type]
                valid_json_objects.append(json_data_set)

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
                operation_desc=f"entities validate ({_state.violations} failed)",
                summary=summary,
                error=validation_message if self.fail_on_violations else None,
                warnings=[validation_message]
                if not self.fail_on_violations and _state.violations
                else [],
            )
        )
        if self.target_mode == TARGET.dataset:
            write_to_dataset(
                dataset_id=f"{context.task.project_id()}:{self.target_dataset}",
                file_resource=io.StringIO(json.dumps(valid_json_objects)),
                context=context.user,
            )
            return None

        return build_entities_from_data(valid_json_objects)

    def _validate_json(self, json: dict, schema: dict) -> bool:
        """Validate JSON"""
        try:
            self._state.increment_total()
            validate(instance=json, schema=schema)
        except ValidationError as e:
            self._state.add_violations_message(f"{e.json_path}: {e.message}")
            return False
        return True

    @staticmethod
    def _get_json_dataset_content(context: ExecutionContext, dataset: str) -> dict | list[dict]:
        """Get json dataset content"""
        dataset_id = f"{context.task.project_id()}:{dataset}"
        project_id, task_id = split_task_id(dataset_id)
        task_meta_data = get_task_metadata(project_id, task_id, context.user)
        resource_name = str(task_meta_data["data"]["parameters"]["file"]["value"])
        response = get_resource_response(project_id, resource_name)
        return response.json()  # type: ignore[no-any-return]

    def _convert_entities_to_json(
        self, inputs: Sequence[Entities], path_to_entities: dict[str, Entities], path: str = ""
    ) -> Generator[dict[str, Any], None, None]:
        """Convert a sequence of Entities into JSON-like dictionaries using recursive traversal."""
        for entities in inputs:
            # Initialize path-to-entities map for the root level
            if not path:
                path_to_entities = {"": entities}

            # Map sub-entities to their paths
            if entities.sub_entities:
                for sub_entity in entities.sub_entities:
                    sub_path = f"{path}/{sub_entity.schema.path_to_root.path}"
                    path_to_entities[sub_path] = sub_entity

            # Process individual entities
            for item in entities.entities:
                json_obj = {}
                for index, schema_path in enumerate(entities.schema.paths):
                    value = list(item.values[index])

                    if schema_path.is_relation:
                        # Handle relational sub-entities
                        related_entity_path = f"{path}/{schema_path.path}"
                        related_entity = path_to_entities.get(related_entity_path)
                        if related_entity:
                            # Recursively process related entities and fetch the first result
                            related_gen = self._convert_entities_to_json(
                                [related_entity],
                                path_to_entities,
                                related_entity_path,
                            )
                            value = [next(related_gen)]

                    # Assign values based on whether the path is single-value or multi-value
                    if schema_path.is_single_value:
                        json_obj[schema_path.path] = value.pop() if value else None
                    else:
                        json_obj[schema_path.path] = value

                yield json_obj
        yield {}
