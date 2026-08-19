"""Plugin tests."""

import json
from collections.abc import Generator
from dataclasses import dataclass
from os import environ
from pathlib import Path

import pytest
from cmem_client.client import Client
from cmem_client.models.dataset import Dataset, DatasetData, DatasetMetadata
from cmem_client.models.project import Project
from cmem_client.repositories.protocols.import_item import ImportConflictPolicy
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


def validate_test_source_target_dataset(project: TestSetup) -> None:
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
