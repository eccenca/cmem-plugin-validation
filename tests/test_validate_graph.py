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
from cmem_plugin_base.dataintegration.parameter.graph import GraphParameterType
from cmem_plugin_base.testing import TestExecutionContext, TestPluginContext

from cmem_plugin_validation.validate_graph.task import CONTEXT_GRAPH_CLASSES, ValidateGraph
from tests.fixtures import FIXTURE_DIR

N_TRIPLES = GraphExportConfig(serialization=GraphsRepository.formats["n-triples"])

needs_cmem = pytest.mark.skipif(
    environ.get("CMEM_BASE_URI", "") == "",
    reason="Needs eccenca Corporate Memory configuration",
)


def get_client() -> Client:
    """Get a fresh client

    Clients are created per operation on purpose: a client keeps its HTTP connections
    alive in a pool, and a connection which idles while a validation process runs is
    closed by the server before it is used again.
    """
    return Client.from_context(context=TestExecutionContext())


def _get_triple_count(graph: str) -> int:
    """Export a graph as n-triples and count the lines

    A graph which is not there fails the test instead of counting as empty. The two
    are different findings: a validation which writes no result is a plugin problem,
    while a result graph vanishing from the deployment is not, and reporting the
    second as a count of zero hides it behind an assertion about numbers.
    """
    client = get_client()
    if graph not in client.graphs:
        pytest.fail(f"Graph <{graph}> does not exist in the deployment")
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
    ontology_graph = "http://docker.localhost/ontology-for-persons/"
    ontology_file = FIXTURE_DIR / "ontology.ttl"


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


@pytest.fixture
def ontology_graph() -> Generator[str, Any]:
    """Provide a graph which is typed as an owl:Ontology"""
    _ = TestSetup()
    client = get_client()
    client.graphs.import_item(
        path=_.ontology_file, key=_.ontology_graph, on_conflict=ImportConflictPolicy.REPLACE
    )
    yield _.ontology_graph
    _delete_graphs(_.ontology_graph)


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
    assert _.result_graph not in get_client().graphs, "result graph should not exist yet"
    task.execute(context=TestExecutionContext(), inputs=[])
    result_graph_triples = _get_triple_count(_.result_graph)
    assert result_graph_triples > 0, "result graph should hold a result set"
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


@needs_cmem
def test_ontology_as_context_graph(ontology_graph: str) -> None:
    """Test that an ontology graph is offered as a context graph"""
    parameter_type = GraphParameterType(
        classes=CONTEXT_GRAPH_CLASSES,
        show_di_graphs=False,
        show_graphs_without_class=True,
        show_system_graphs=True,
        allow_only_autocompleted_values=False,
    )
    values = {
        _.value
        for _ in parameter_type.autocomplete(
            query_terms=[], depend_on_parameter_values=[], context=TestPluginContext()
        )
    }
    assert ontology_graph in values
