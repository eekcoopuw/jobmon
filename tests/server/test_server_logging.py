import pytest

import json


def test_capsys(in_memory_jsm_jqs, capsys):
    from jobmon.client.templates.unknown_workflow import UnknownWorkflow
    from jobmon.client.api import BashTask

    wf = UnknownWorkflow("test_server")
    task_a = BashTask("echo r", executor_class="SequentialExecutor")
    wf.add_task(task_a)
    wf._bind()
    wf.requester.send_request(
        app_route='/client/workflow',
        message={
            "tool_version_id": wf.tool_version_id,
            "dag_id": 'blah',
            "workflow_args_hash": wf.workflow_args_hash,
            "task_hash": wf.task_hash
        },
        request_type='get'
    )
    captured = capsys.readouterr()
    logs = captured.err.split('\n')
    for log in logs:
        if 'blueprint' in log:
            val = json.loads(log)
            assert 'jobmon.server.web' in val['blueprint']
