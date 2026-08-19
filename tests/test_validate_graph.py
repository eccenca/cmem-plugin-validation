"""Test KG Validation Workflow task"""

from collections.abc import Generator
from dataclasses import dataclass
from os import environ
from pathlib import Path
from tempfile import TemporaryDirectory
from types import NoneType
from typing import Any

import pytest
from cmem_client.client import Client
from cmem_client.repositories.graphs import GraphExportConfig, GraphsRepository
from cmem_client.repositories.protocols.import_item import ImportConflictPolicy
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.testing import TestExecutionContext

from cmem_plugin_validation.validate_graph.task import ValidateGraph
from tests.fixtures import FIXTURE_DIR

N_TRIPLES = GraphExportConfig(serialization=GraphsRepository.formats["n-triples"])


def get_client() -> Client:
    """Get a fresh client

    Clients are created per operation on purpose: a client keeps its HTTP connections
    alive in a pool, and a connection which idles while a validation process runs is
    closed by the server before it is used again.
    """
    return Client.from_context(context=TestExecutionContext())


def _get_triple_count(graph: str) -> int:
    """Export a graph as n-triples and count the lines"""
    client = get_client()
    if graph not in client.graphs:
        return 0
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "graph.nt"
        client.graphs.export_item(key=graph, path=path, replace=True, configuration=N_TRIPLES)
        return len(path.read_text(encoding="utf-8").splitlines())


def _delete_graphs(*graphs: str) -> None:
    """Delete graphs, ignoring the ones which are not there"""
    client = get_client()
    for graph in graphs:
        client.graphs.delete_item(key=graph, skip_if_missing=True)


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
def test_setup() -> Generator[TestSetup, Any]:
    """Provide Test Setup"""
    if environ.get("CMEM_BASE_URI", "") == "":
        pytest.skip("Needs CMEM configuration")
    _ = TestSetup()
    client = get_client()
    for graph, file in ((_.persons_graph, _.persons_file), (_.shapes_graph, _.shapes_file)):
        client.graphs.import_item(path=file, key=graph, on_conflict=ImportConflictPolicy.REPLACE)
    client.graphs.delete_item(key=_.result_graph, skip_if_missing=True)
    yield _
    # purge setup
    _delete_graphs(_.persons_graph, _.shapes_graph, _.result_graph)


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
    assert entities[0].values[1] == ["http://example.org/persons/2"], (
        "focus node of the only violation should be person 2"
    )


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
    assert _get_triple_count(_.result_graph) == result_graph_triples * 2, (
        "result graph should have two equal result sets"
    )
    task.clear_result_graph = True
    task.execute(context=TestExecutionContext(), inputs=[])
    assert _get_triple_count(_.result_graph) == result_graph_triples, (
        "result graph should have as single result sets again"
    )


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
    assert task.execute(context=TestExecutionContext(), inputs=[]) is None, (
        "Should no violations, since no person was validated"
    )
