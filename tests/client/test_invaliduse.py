import logging

from jobmon.requester import Requester

"""This is the test the HTTP 400 errors.
"""

logger = logging.getLogger(__name__)


def test_add_tool(db_cfg, client_env):
    # @jobmon_client.route('/tool', methods=['POST'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/tool',
        message={},
        request_type='post')
    assert rc == 400


def test_get_tool_versions(db_cfg, client_env):
    # @jobmon_client.route('/tool/<tool_id>/tool_versions', methods=['GET'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/tool/abc/tool_versions',
        message={},
        request_type='get')
    assert rc == 400


def test_add_tool_version(db_cfg, client_env):
    # @jobmon_client.route('/tool_version', methods=['POST'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/tool_version',
        message={'paramter_does_not_exist': 'abc'},
        request_type='post')
    assert rc == 400
    rc, response = requester.send_request(
        app_route='/tool_version',
        message={'tool_id': 'abc'},
        request_type='post')
    assert rc == 400


def test_get_task_template(db_cfg, client_env):
    # @jobmon_client.route('/task_template', methods=['GET'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/task_template',
        message={'paramter_does_not_exist': 'abc'},
        request_type='get')
    assert rc == 400


def test_add_task_template(db_cfg, client_env):
    # @jobmon_client.route('/task_template', methods=['POST'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/task_template',
        message={'paramter_does_not_exist': 'abc'},
        request_type='post')
    assert rc == 400
    rc, response = requester.send_request(
        app_route='/task_template',
        message={'tool_version_id': 'abc'},
        request_type='post')
    assert rc == 400


def test_add_task_template_version(db_cfg, client_env):
    # @jobmon_client.route('/task_template/<task_template_id>/add_version', methods=['POST'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/task_template/abc/add_version',
        message={},
        request_type='post')
    assert rc == 400


def test_get_task_id_and_status(db_cfg, client_env):
    # @jobmon_client.route('/task', methods=['GET'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/task',
        message={'workflow_id': 'abc'},
        request_type='get')
    assert rc == 400
    rc, response = requester.send_request(
        app_route='/task',
        message={'node_id': 'abc'},
        request_type='get')
    assert rc == 400
    rc, response = requester.send_request(
        app_route='/task',
        message={'task_args_hash': 'abc'},
        request_type='get')
    assert rc == 400


def test_add_task_id_and_status(db_cfg, client_env):
    # @jobmon_client.route('/task', methods=['POST'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/task',
        message={'workflow_id': 'abc'},
        request_type='post')
    assert rc == 400
    rc, response = requester.send_request(
        app_route='/task',
        message={'node_id': 'abc'},
        request_type='post')
    assert rc == 400


def test_update_task_parameters(db_cfg, client_env):
    # @jobmon_client.route('/task/<task_id>/update_parameters', methods=['PUT'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/task/abc/update_parameters',
        message={},
        request_type='put')
    assert rc == 400


def test_update_task_attribute(db_cfg, client_env):
    # @jobmon_client.route('/task/<task_id>/task_attributes', methods=['PUT'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/task/abc/task_attributes',
        message={},
        request_type='put')
    assert rc == 400


def test_add_workflow(db_cfg, client_env):
    # @jobmon_client.route('/workflow', methods=['POST'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/workflow',
        message={'dag_id': 'abc'},
        request_type='post')
    assert rc == 400
    rc, response = requester.send_request(
        app_route='/workflow',
        message={'tool_version_id': 'abc'},
        request_type='post')
    assert rc == 400
    rc, response = requester.send_request(
        app_route='/workflow',
        message={'workflow_args_hash': 'abc'},
        request_type='post')
    assert rc == 400
    rc, response = requester.send_request(
        app_route='/workflow',
        message={'task_hash': 'abc'},
        request_type='post')
    assert rc == 400


def test_get_matching_workflows_by_workflow_args(db_cfg, client_env):
    # @jobmon_client.route('/workflow/<workflow_args_hash>', methods=['GET'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/workflow/abcdefg',
        message={},
        request_type='get')
    assert rc == 400


def test_workflow_attributes(db_cfg, client_env):
    # @jobmon_client.route('/workflow/<workflow_id>/workflow_attributes', methods=['PUT'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/workflow/abc/workflow_attributes',
        message={},
        request_type='put')
    assert rc == 400


def test_add_workflow_rund(db_cfg, client_env):
    # @jobmon_client.route('/workflow_run', methods=['POST'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/workflow_run',
        message={},
        request_type='post')
    assert rc == 400


def test_terminate_workflow_run(db_cfg, client_env):
    # @jobmon_client.route('/workflow_run/<workflow_run_id>/terminate', methods=['PUT'])
    requester = Requester(client_env, logger)
    rc, response = requester.send_request(
        app_route='/workflow_run/abc/terminate',
        message={},
        request_type='put')
    assert rc == 400
