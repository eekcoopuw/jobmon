import pytest

from jobmon.client.workflow.tool import Tool, InvalidToolVersionError


def test_create_tool(db_cfg, env_var):
    t1 = Tool.create_tool(name="foo")
    assert t1.name == "foo"
    # check that we have an id
    assert t1.id is not None
    # check that a tool version got created
    assert len(t1.tool_version_ids) == 1

    # check that we can initialize with just the name
    t2 = Tool("foo")
    assert t2.id == t1.id
    assert t2.active_tool_version_id == t1.active_tool_version_id


def test_create_tool_version(db_cfg, env_var):
    t1 = Tool.create_tool(name="bar")
    orig_tool_version = t1.active_tool_version_id

    # create a new version
    new_tool_version_id = t1.create_new_tool_version()
    assert len(t1.tool_version_ids) == 2

    # check that the new version is now active
    assert t1.active_tool_version_id == new_tool_version_id

    # reassign the activer version to the old value and confirm it works
    t1.active_tool_version_id = orig_tool_version
    assert t1.active_tool_version_id == orig_tool_version

    # try to assign to an invalid value
    with pytest.raises(InvalidToolVersionError):
        t1.active_tool_version_id = 0

    # use latest to reassign and confirm it works
    t1.active_tool_version_id = "latest"
    assert t1.active_tool_version_id == new_tool_version_id
