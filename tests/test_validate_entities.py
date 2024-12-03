"""Plugin tests."""
import io

import pytest
from cmem.cmempy.workspace.projects.datasets.dataset import make_new_dataset
from cmem.cmempy.workspace.projects.project import make_new_project, delete_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource
from cmem_plugin_validation.validate_entities.task import SOURCE, TARGET, ValidateEntity
from tests.utils import needs_cmem

PROJECT_NAME = "validate_entities_test_project"
DATASET_NAME = "sample_dataset"
RESOURCE_NAME = "sample_dataset.json"
DATASET_TYPE = "json"

@pytest.fixture()
def project() -> object:
    """Provide the DI build project incl. assets."""
    make_new_project(PROJECT_NAME)
    make_new_dataset(
        project_name=PROJECT_NAME,
        dataset_name=DATASET_NAME,
        dataset_type=DATASET_TYPE,
        parameters={"file": RESOURCE_NAME},
        autoconfigure=False,
    )
    with io.StringIO('{"key": "value"}') as response_file:
        create_resource(
            project_name=PROJECT_NAME,
            resource_name=RESOURCE_NAME,
            file_resource=response_file,
            replace=True,
        )
    yield {"project": PROJECT_NAME, "dataset": DATASET_NAME, "resource": RESOURCE_NAME}
    delete_project(PROJECT_NAME)


@needs_cmem
def test_configuration(project):
    """Test missing source configuration"""
    with pytest.raises(ValueError, match="When using the source mode 'dataset', you need to select a Source JSON Dataset."):
        ValidateEntity(source_mode=SOURCE.dataset, target_mode=TARGET.entities, json_schema_dataset="", fail_on_violations=False)
    with pytest.raises(ValueError, match="When using the target mode 'dataset', you need to select a Target JSON dataset."):
        ValidateEntity(source_mode=SOURCE.entities, target_mode=TARGET.dataset, json_schema_dataset="", fail_on_violations=False)

