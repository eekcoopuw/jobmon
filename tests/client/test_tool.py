from jobmon.client.tool import Tool

import pytest


def test_create_tool(db_cfg, client_env):
    """test that we can create a tool and recreate it with identical params and
    get the same ID"""
    t1 = Tool(name="foo")
    assert t1.name == "foo"
    # check that we have an id
    assert t1.id is not None
    # check that a tool version got created
    assert len(t1.tool_versions) == 1

    # check that we can initialize with just the name
    t2 = Tool("foo")
    assert t2.id == t1.id
    assert t2.active_tool_version.id == t1.active_tool_version.id


def test_create_tool_version(db_cfg, client_env):
    """test that we create a new tool version"""

    t1 = Tool(name="bar")
    orig_tool_version = t1.active_tool_version.id

    # create a new version
    new_tool_version_id = t1.get_new_tool_version()
    assert len(t1.tool_versions) == 2

    # check that the new version is now active
    assert t1.active_tool_version.id == new_tool_version_id

    # reassign the activer version to the old value and confirm it works
    t1.set_active_tool_version_id(orig_tool_version)
    assert t1.active_tool_version.id == orig_tool_version

    # try to assign to an invalid value
    with pytest.raises(ValueError):
        t1.set_active_tool_version_id(0)

    # use latest to reassign and confirm it works
    t1.set_active_tool_version_id("latest")
    assert t1.active_tool_version.id == new_tool_version_id
