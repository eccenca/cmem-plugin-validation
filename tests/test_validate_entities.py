"""Plugin tests."""

import json
from collections.abc import Generator
from dataclasses import dataclass
from os import environ
from pathlib import Path
from types import SimpleNamespace
from typing import IO, Any

import pytest
from cmem_client.client import Client
from cmem_client.models.dataset import Dataset, DatasetData, DatasetMetadata
from cmem_client.models.project import Project
from cmem_client.repositories.protocols.import_item import ImportConflictPolicy
from cmem_plugin_base.dataintegration.context import ExecutionContext, ReportContext
from cmem_plugin_base.dataintegration.entity import Entities, Entity, EntityPath, EntitySchema
from cmem_plugin_base.testing import TestExecutionContext

from cmem_plugin_validation.validate_entities.task import SOURCE, TARGET, ValidateEntity
from tests.fixtures import FIXTURE_DIR


@dataclass
class TestSetup:
    """Class for providing Validation Test Setup"""

    schema_dataset_file: Path = FIXTURE_DIR / "schema.json"
    schema_dataset: str = "schema_dataset"
    valid_source_dataset_file: Path = FIXTURE_DIR / "source.valid.json"
    invalid_source_dataset_file: Path = FIXTURE_DIR / "source.invalid.json"
    valid_source_dataset: str = "valid_source_dataset"
    invalid_source_dataset: str = "invalid_source_dataset"
    target_dataset_file: str = "target.json"
    target_dataset: str = "target_dataset"
    project_name: str = "validate_entities_test_project"
    valid_source_object_count = 2


def get_client(project_name: str) -> Client:
    """Get a fresh client

    Clients are created per operation on purpose: a client keeps its HTTP connections
    alive in a pool, and a connection which idles while a test runs is closed by the
    server before it is used again.
    """
    return Client.from_context(context=TestExecutionContext(project_id=project_name))


def _make_dataset(client: Client, project_name: str, dataset_name: str, file_name: str) -> None:
    """Create a new JSON dataset in a project"""
    client.datasets.create_item(
        Dataset(
            id=dataset_name,
            project=project_name,
            data=DatasetData(type="json", parameters={"file": file_name}),
            metadata=DatasetMetadata(label=dataset_name),
        )
    )


@pytest.fixture
def project() -> Generator[TestSetup]:
    """Provide the DI build project incl. assets."""
    _ = TestSetup()
    client = get_client(_.project_name)
    # a run which dies before the teardown below leaves the project behind, and
    # create_item then fails for every later run - so (re-)create it
    client.projects.delete_item(_.project_name, skip_if_missing=True)
    client.projects.create_item(Project(name=_.project_name))
    _make_dataset(client, _.project_name, _.target_dataset, _.target_dataset_file)
    for dataset_name, dataset_file in (
        (_.valid_source_dataset, _.valid_source_dataset_file),
        (_.invalid_source_dataset, _.invalid_source_dataset_file),
        (_.schema_dataset, _.schema_dataset_file),
    ):
        _make_dataset(client, _.project_name, dataset_name, dataset_file.name)
        client.files.import_item(
            path=dataset_file,
            key=f"{_.project_name}:{dataset_file.name}",
            on_conflict=ImportConflictPolicy.REPLACE,
        )
    yield _
    get_client(_.project_name).projects.delete_item(_.project_name)


needs_cmem = pytest.mark.skipif(
    environ.get("CMEM_BASE_URI", "") == "", reason="Needs CMEM configuration"
)


@needs_cmem
def test_configuration(project: TestSetup) -> None:
    """Test configuration setup"""
    _ = project
    with pytest.raises(
        ValueError,
        match=r"When using the source mode 'dataset', you need to select a Source JSON Dataset.",
    ):
        ValidateEntity(
            source_mode=SOURCE.dataset,
            target_mode=TARGET.entities,
            json_schema_dataset="",
            fail_on_violations=False,
        )
    with pytest.raises(
        ValueError,
        match=r"When using the target mode 'dataset', you need to select a Target JSON dataset.",
    ):
        ValidateEntity(
            source_mode=SOURCE.entities,
            target_mode=TARGET.dataset,
            json_schema_dataset="",
            fail_on_violations=False,
        )


@needs_cmem
def test_execute_with_source_dataset(project: TestSetup) -> None:
    """Test source dataset mode"""
    _ = project
    entities = ValidateEntity(
        source_mode=SOURCE.dataset,
        target_mode=TARGET.entities,
        json_schema_dataset=_.schema_dataset,
        fail_on_violations=False,
        source_dataset=_.valid_source_dataset,
    ).execute([], TestExecutionContext(project_id=_.project_name))
    assert entities is not None
    if entities is not None:
        assert len(list(entities.entities)) == _.valid_source_object_count

    entities = ValidateEntity(
        source_mode=SOURCE.dataset,
        target_mode=TARGET.entities,
        json_schema_dataset=_.schema_dataset,
        fail_on_violations=False,
        source_dataset=_.invalid_source_dataset,
    ).execute([], TestExecutionContext(project_id=_.project_name))
    assert entities is not None
    if entities is not None:
        assert len(list(entities.entities)) == 1


@needs_cmem
def test_source_and_target_dataset(project: TestSetup) -> None:
    """Test source and target dataset mode"""
    _ = project

    ValidateEntity(
        source_mode=SOURCE.dataset,
        target_mode=TARGET.dataset,
        json_schema_dataset=_.schema_dataset,
        fail_on_violations=False,
        source_dataset=_.valid_source_dataset,
        target_dataset=_.target_dataset,
    ).execute([], TestExecutionContext(project_id=_.project_name))

    client = get_client(_.project_name)
    data = json.loads(client.files.read(f"{_.project_name}:{_.target_dataset_file}"))
    assert len(data) == _.valid_source_object_count


class _FakeDatasetItem:
    """Stand-in for the dataset item cmem_client.datasets.get_item() returns"""

    def __init__(self, file_name: str) -> None:
        self.data = SimpleNamespace(parameters={"file": file_name})


class _FakeDatasets:
    """Stand-in for cmem_client.client.Client.datasets"""

    def __init__(self, schema_file_name: str, written: dict[str, bytes]) -> None:
        self._schema_file_name = schema_file_name
        self._written = written

    def get_item(self, project_id: str, dataset_id: str) -> _FakeDatasetItem:
        """Return the schema dataset's file name, the only lookup task.py performs"""
        return _FakeDatasetItem(self._schema_file_name)

    def post_file_resource(
        self, project_id: str, dataset_id: str, file_resource: IO[bytes]
    ) -> None:
        """Record the raw bytes written to the target dataset instead of uploading them"""
        _ = project_id, dataset_id
        self._written["content"] = file_resource.read()


class _FakeFiles:
    """Stand-in for cmem_client.client.Client.files"""

    def __init__(self, schema_bytes: bytes) -> None:
        self._schema_bytes = schema_bytes

    def read(self, key: str) -> bytes:
        """Return the JSON schema content, the only file this task reads"""
        _ = key
        return self._schema_bytes


class _FakeClient:
    """Stand-in for cmem_client.client.Client, avoiding a real Corporate Memory connection"""

    def __init__(self, schema_bytes: bytes, schema_file_name: str, written: dict[str, bytes]):
        self.datasets = _FakeDatasets(schema_file_name, written)
        self.files = _FakeFiles(schema_bytes)


class _StubExecutionContext(ExecutionContext):
    """An execution context which needs no Corporate Memory connection.

    task.py only reads ``context.task.project_id()`` and calls
    ``context.report.update()`` - it never touches ``context.user``.
    """

    def __init__(self, project_id: str) -> None:
        self.report = ReportContext()
        self.task = SimpleNamespace(project_id=lambda: project_id)


def test_target_dataset_keeps_unicode_characters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that non-ASCII characters in entity values are not escaped in the target dataset

    Uses a fake Client so this runs without a Corporate Memory connection: task.py only
    calls Client.datasets.get_item() and Client.files.read() to resolve the JSON schema,
    and Client.datasets.post_file_resource() to write the target dataset.
    """
    schema = json.dumps({"type": "object", "properties": {"city": {"type": "string"}}}).encode()
    written: dict[str, Any] = {}
    monkeypatch.setattr(
        "cmem_plugin_validation.validate_entities.task.Client.from_context",
        lambda context: _FakeClient(schema, "schema.json", written),
    )

    entities = Entities(
        entities=[Entity(uri="urn:x-1", values=[["Köln"]])],
        schema=EntitySchema(type_uri="", paths=[EntityPath(path="city", is_single_value=True)]),
    )
    ValidateEntity(
        source_mode=SOURCE.entities,
        target_mode=TARGET.dataset,
        json_schema_dataset="schema_dataset",
        fail_on_violations=True,
        target_dataset="target_dataset",
    ).execute([entities], _StubExecutionContext(project_id="validate_entities_unit_test"))

    raw_content = written["content"].decode("utf-8")
    assert "\\u00f6" not in raw_content
    assert json.loads(raw_content) == [{"city": "Köln"}]
