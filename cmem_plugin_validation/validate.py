"""Graph validation workflow task"""

import json
from collections.abc import Sequence
from time import sleep

from cmem.cmempy.dp.shacl import validation
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import (
    Entities,
)
from cmem_plugin_base.dataintegration.parameter.graph import GraphParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import FixedNumberOfInputs
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access
from requests import HTTPError

from cmem_plugin_validation.state import State

DOCUMENTATION = """
A validation process verifies, that resources in a specific graph are valid according to
the node shapes in a shape catalog graph.
"""


@Plugin(
    label="Validate Knowledge Graph",
    description="Validate resources in a context graph based on node and property"
    " shapes from a Shape Catalog graph.",
    documentation=DOCUMENTATION,
    parameters=[
        PluginParameter(
            name="context_graph",
            label="Context Graph",
            description="...",
            param_type=GraphParameterType(
                show_di_graphs=True,
                show_graphs_without_class=True,
                show_system_graphs=True,
                allow_only_autocompleted_values=False,
            ),
        ),
        PluginParameter(
            name="shape_graph",
            label="Shape Graph",
            description="...",
            param_type=GraphParameterType(
                classes=["https://vocab.eccenca.com/shui/ShapeCatalog"], show_system_graphs=True
            ),
            default_value="https://vocab.eccenca.com/shacl/",
        ),
        PluginParameter(
            name="fail_on_violations",
            label="Fail on Violations",
            description="...",
        ),
    ],
)
class ValidateGraph(WorkflowPlugin):
    """Validate resources in a graph"""

    def __init__(
        self, context_graph: str, shape_graph: str, fail_on_violations: bool = True
    ) -> None:
        self.context_graph = context_graph
        self.shape_graph = shape_graph
        self.fail_on_violations = fail_on_violations
        self.input_ports = FixedNumberOfInputs([])
        self.output_port = None

    def execute(
        self,
        inputs: Sequence[Entities],  # noqa: ARG002
        context: ExecutionContext,
    ) -> None:
        """Run the workflow operator."""
        self.log.info("Start validation task.")
        setup_cmempy_user_access(context=context.user)
        try:
            process_id = validation.start(
                context_graph=self.context_graph, shape_graph=self.shape_graph
            )
        except HTTPError as error_message:
            context.report.update(
                ExecutionReport(
                    error=json.loads(error_message.response.text)["detail"],
                )
            )
            return
        state = State(id_=process_id)
        while True:
            sleep(1)
            setup_cmempy_user_access(context=context.user)
            state.refresh()
            if context.workflow.status() != "Running":
                validation.cancel(batch_id=process_id)
                context.report.update(
                    ExecutionReport(
                        entity_count=state.completed,
                        operation="read",
                        operation_desc=f"/ {state.total} Resources validated (cancelled)",
                    )
                )
                self.log.info("End validation task (Cancelled Workflow).")
                return
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
            f"Found {state.violations} Violations"
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
        self.log.info("End validation task.")
