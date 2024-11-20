"""Test KG Validation Workflow task"""

from collections.abc import Generator
from dataclasses import dataclass
from os import environ
from types import NoneType
from typing import Any

import cmem.cmempy.dp.proxy.graph as graph_api
import pytest
from cmem_plugin_base.dataintegration.entity import Entities

from cmem_plugin_validation.validate_graph.task import ValidateGraph
from tests.fixtures import FIXTURE_DIR
from tests.utils import TestExecutionContext


def _get_triple_count(graph: str) -> int:
    """Fetch a graph as n-triples and count the lines"""
    lines = graph_api.get(graph=graph, accept="application/n-triples").text.splitlines()
    return len(lines)


@dataclass
class TestSetup:
    """Class for providing Validation Test Setup"""

    existing_graph = "https://ns.eccenca.com/data/queries/"
    not_existing_graph = "https://example.org/not-here"
    persons_graph = "http://example.org/persons/"
    persons_file = FIXTURE_DIR / "persons.ttl"
    shapes_graph = "http://docker.localhost/shapes-for-persons/"
    shapes_file = FIXTURE_DIR / "shapes.ttl"
    result_graph = "http://docker.localhost/results/"


@pytest.fixture
def test_setup() -> Generator[TestSetup, Any, None]:
    """Provide Test Setup"""
    if environ.get("CMEM_BASE_URI", "") == "":
        pytest.skip("Needs CMEM configuration")
    _ = TestSetup()
    graph_api.post_streamed(replace=True, file=_.persons_file, graph=_.persons_graph)
    graph_api.post_streamed(replace=True, file=_.shapes_file, graph=_.shapes_graph)
    graph_api.delete(graph=_.result_graph)
    yield _
    # purge setup
    graph_api.delete(graph=_.persons_graph)
    graph_api.delete(graph=_.shapes_graph)
    graph_api.delete(graph=_.result_graph)


def test_fails(test_setup: TestSetup) -> None:
    """Test failing task execution"""
    _ = test_setup
    with pytest.raises(RuntimeError) as exception_info:
        ValidateGraph(context_graph="").execute(context=TestExecutionContext(), inputs=[])
    assert "MALFORMED QUERY" in str(exception_info)
    with pytest.raises(RuntimeError) as exception_info:
        ValidateGraph(context_graph=_.not_existing_graph).execute(
            context=TestExecutionContext(), inputs=[]
        )
    assert "Selection query returns empty result set" in str(exception_info)
    with pytest.raises(RuntimeError) as exception_info:
        ValidateGraph(context_graph=_.existing_graph, shape_graph=_.not_existing_graph).execute(
            context=TestExecutionContext(), inputs=[]
        )
    assert "does not exist in graph list" in str(exception_info)


def test_output_results(test_setup: TestSetup) -> None:
    """Test task execution with output results or not"""
    _ = test_setup
    task = ValidateGraph(
        context_graph=_.persons_graph, shape_graph=_.shapes_graph, output_results=False
    )
    result = task.execute(context=TestExecutionContext(), inputs=[])
    assert result is None
    assert isinstance(result, NoneType)
    task = ValidateGraph(
        context_graph=_.persons_graph, shape_graph=_.shapes_graph, output_results=True
    )
    result = task.execute(context=TestExecutionContext(), inputs=[])
    assert isinstance(result, Entities)
    entities = list(result.entities)
    assert len(entities) == 1, "There should be a single violation entity"
    assert entities[0].values[1] == [
        "http://example.org/persons/2"
    ], "focus node of the only violation should be person 2"


def test_safe_as_graph(test_setup: TestSetup) -> None:
    """Test task execution with result graph output and clearance"""
    _ = test_setup
    task = ValidateGraph(
        context_graph=_.persons_graph,
        shape_graph=_.shapes_graph,
        output_results=False,
        result_graph=_.result_graph,
        clear_result_graph=False,
    )
    assert _get_triple_count(_.result_graph) == 0
    task.execute(context=TestExecutionContext(), inputs=[])
    result_graph_triples = _get_triple_count(_.result_graph)
    assert result_graph_triples > 0, "result graph should be empty"
    task.execute(context=TestExecutionContext(), inputs=[])
    assert (
        _get_triple_count(_.result_graph) == result_graph_triples * 2
    ), "result graph should have two equal result sets"
    task.clear_result_graph = True
    task.execute(context=TestExecutionContext(), inputs=[])
    assert (
        _get_triple_count(_.result_graph) == result_graph_triples
    ), "result graph should have as single result sets again"


def test_different_query(test_setup: TestSetup) -> None:
    """Test task execution with different queries"""
    _ = test_setup
    task = ValidateGraph(
        context_graph=_.persons_graph,
        shape_graph=_.shapes_graph,
        output_results=True,
    )
    result = task.execute(context=TestExecutionContext(), inputs=[])
    assert isinstance(result, Entities)
    assert len(list(result.entities)) == 1, "There should be a single violation entity"
    query = """
PREFIX di: <https://vocab.eccenca.com/di/>
SELECT DISTINCT ?resource
FROM <{{context_graph}}>
WHERE {
    ?resource a di:Dataset.
    FILTER isIRI(?resource)
}
"""
    task.sparql_query = query
    assert (
        task.execute(context=TestExecutionContext(), inputs=[]) is None
    ), "Should no violations, since no person was validated"
