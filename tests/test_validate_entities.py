"""Plugin tests."""

import json
from collections.abc import Generator
from dataclasses import dataclass
from os import environ
from pathlib import Path

import pytest
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource, get_resource_response
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


def _make_dataset(project_name: str, dataset_name: str, dataset_file_name: Path) -> None:
    """Create new dataset in project"""
    make_new_dataset(
        project_name=project_name,
        dataset_name=dataset_name,
        dataset_type="json",
        parameters={"file": str(dataset_file_name)},
        autoconfigure=False,
    )
    with dataset_file_name.open("rb") as response_file:
        create_resource(
            project_name=project_name,
            resource_name=str(dataset_file_name),
            file_resource=response_file,
            replace=True,
        )


@pytest.fixture
def project() -> Generator[TestSetup, None, None]:
    """Provide the DI build project incl. assets."""
    _ = TestSetup()
    make_new_project(_.project_name)
    make_new_dataset(
        project_name=_.project_name,
        dataset_name=_.target_dataset,
        dataset_type="json",
        parameters={"file": _.target_dataset_file},
        autoconfigure=False,
    )
    _make_dataset(_.project_name, _.valid_source_dataset, _.valid_source_dataset_file)
    _make_dataset(_.project_name, _.invalid_source_dataset, _.invalid_source_dataset_file)
    _make_dataset(_.project_name, _.schema_dataset, _.schema_dataset_file)
    yield _
    delete_project(_.project_name)


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

    with get_resource_response(_.project_name, _.target_dataset_file) as response:
        data = json.loads(response.content)
        assert len(data) == _.valid_source_object_count
