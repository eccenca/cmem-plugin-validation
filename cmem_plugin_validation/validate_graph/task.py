"""Graph validation workflow task"""

import json
from collections.abc import Sequence
from time import sleep

from cmem.cmempy.dp.proxy import graph as graph_api
from cmem.cmempy.dp.shacl import validation
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Icon, Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import (
    Entities,
    EntityPath,
    EntitySchema,
)
from cmem_plugin_base.dataintegration.parameter.graph import GraphParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import (
    FixedNumberOfInputs,
    FixedSchemaPort,
)
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access
from cmem_plugin_base.dataintegration.utils.entity_builder import build_entities_from_data
from requests import HTTPError

from cmem_plugin_validation.validate_graph.state import State

DOCUMENTATION = """
A graph validation process verifies, that resources in a specific graph are valid according to
the node shapes in a shape catalog graph.
"""

DEFAULT_SHAPE_GRAPH = "https://vocab.eccenca.com/shacl/"
DEFAULT_RESULT_GRAPH = ""
DEFAULT_CLEAR_RESULT_GRAPH = False
DEFAULT_FAIL_ON_VIOLATION = False
DEFAULT_OUTPUT_RESULTS = True


@Plugin(
    label="Validate Knowledge Graph",
    plugin_id="cmem_plugin_validation-validate-ValidateGraph",
    icon=Icon(file_name="icon.svg", package=__package__),
    description="Use shapes to validate resources in a knowledge graph.",
    documentation=DOCUMENTATION,
    parameters=[
        PluginParameter(
            name="context_graph",
            label="Context Graph",
            description="This graph holds the resources you want to validate.",
            param_type=GraphParameterType(
                show_di_graphs=False,
                show_graphs_without_class=True,
                show_system_graphs=True,
                allow_only_autocompleted_values=False,
            ),
        ),
        PluginParameter(
            name="shape_graph",
            label="Shape graph",
            description="This graph holds the shapes you want to use for validation.",
            param_type=GraphParameterType(
                classes=["https://vocab.eccenca.com/shui/ShapeCatalog"], show_system_graphs=True
            ),
            default_value=DEFAULT_SHAPE_GRAPH,
        ),
        PluginParameter(
            name="result_graph",
            label="Result graph",
            description="In this graph, the validation results are materialized. "
            "If left empty, results are not materialized.",
            default_value=DEFAULT_RESULT_GRAPH,
            param_type=GraphParameterType(
                show_di_graphs=False,
                show_graphs_without_class=True,
                show_system_graphs=False,
                allow_only_autocompleted_values=False,
            ),
        ),
        PluginParameter(
            name="clear_result_graph",
            label="Clear result graph before validation",
            default_value=DEFAULT_CLEAR_RESULT_GRAPH,
        ),
        PluginParameter(
            name="fail_on_violations",
            label="Fail workflow on violations",
            default_value=DEFAULT_FAIL_ON_VIOLATION,
        ),
        PluginParameter(
            name="output_results",
            label="Output violations as entities",
            default_value=DEFAULT_OUTPUT_RESULTS,
        ),
    ],
)
class ValidateGraph(WorkflowPlugin):
    """Validate resources in a graph"""

    def __init__(  # noqa: PLR0913
        self,
        context_graph: str,
        shape_graph: str = DEFAULT_SHAPE_GRAPH,
        result_graph: str = DEFAULT_RESULT_GRAPH,
        clear_result_graph: bool = DEFAULT_CLEAR_RESULT_GRAPH,
        fail_on_violations: bool = DEFAULT_FAIL_ON_VIOLATION,
        output_results: bool = DEFAULT_OUTPUT_RESULTS,
    ) -> None:
        self.context_graph = context_graph
        self.shape_graph = shape_graph
        self.result_graph = result_graph
        self.fail_on_violations = fail_on_violations
        self.clear_result_graph = clear_result_graph
        self.output_results = output_results
        self.input_ports = FixedNumberOfInputs([])
        if self.output_results:
            self.output_port = FixedSchemaPort(schema=self.output_schema)
        else:
            self.output_port = None

    @property
    def output_schema(self) -> EntitySchema:
        """The violations schema"""
        return EntitySchema(
            type_uri="https://vocab.eccenca.com/validation/Violation",
            paths=[
                EntityPath(path="path", is_single_value=True),
                EntityPath(path="focusNode", is_single_value=True),
                EntityPath(path="source", is_single_value=True),
                EntityPath(path="severity", is_single_value=True),
                EntityPath(path="messages", is_single_value=True),
                EntityPath(path="reportEntryConstraintMessageTemplate", is_single_value=True),
            ],
        )

    def execute(
        self,
        inputs: Sequence[Entities],  # noqa: ARG002
        context: ExecutionContext,
    ) -> Entities | None:
        """Run the workflow operator."""
        self.log.info("Start validation task.")
        setup_cmempy_user_access(context=context.user)
        if self.clear_result_graph and self.result_graph:
            graph_api.delete(graph=self.result_graph)
        try:
            process_id = validation.start(
                context_graph=self.context_graph,
                shape_graph=self.shape_graph,
                result_graph=self.result_graph if self.result_graph else None,
            )
        except HTTPError as error:
            context.report.update(
                ExecutionReport(
                    error=json.loads(error.response.text)["detail"],
                )
            )
            raise RuntimeError(json.loads(error.response.text)["detail"]) from error
        state = State(id_=process_id)
        while True:
            sleep(1)
            setup_cmempy_user_access(context=context.user)
            state.refresh()
            if context.workflow and context.workflow.status() != "Running":
                validation.cancel(batch_id=process_id)
                context.report.update(
                    ExecutionReport(
                        entity_count=state.completed,
                        operation="read",
                        operation_desc=f"/ {state.total} Resources validated (cancelled)",
                    )
                )
                self.log.info("End validation task (Cancelled Workflow).")
                return None
            if state.status in (validation.STATUS_SCHEDULED, validation.STATUS_RUNNING):
                # when reported as running or scheduled, start another loop
                context.report.update(
                    ExecutionReport(
                        entity_count=state.completed,
                        operation="read",
                        operation_desc=f"/ {state.total} Resources validated",
                    )
                )
                continue
            # when reported as finished, error or cancelled break out
            break
        summary: list[tuple[str, str]] = [
            (data_key, str(state.data[data_key])) for data_key in state.data
        ]
        validation_message = (
            f"Found {state.violations} Violations "
            f"in {state.with_violations} / {state.total} Resources."
        )
        context.report.update(
            ExecutionReport(
                entity_count=state.with_violations,
                operation="read",
                operation_desc=f"/ {state.total} Resources have violations",
                summary=summary,
                error=validation_message if self.fail_on_violations else None,
                warnings=[validation_message] if not self.fail_on_violations else None,
            )
        )
        if not self.output_results:
            return None

        violations = []
        for result in list(validation.get(batch_id=process_id)["results"]):
            resource_iri = result.get("resourceIri")
            for _ in result["violations"]:
                violation = dict(_)
                violation["resourceIri"] = resource_iri
                violations.append(violation)
        return build_entities_from_data(data=violations)
