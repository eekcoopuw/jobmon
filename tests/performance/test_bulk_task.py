import logging
import pytest

from jobmon.requester import Requester

logger = logging.getLogger(__name__)


TOTAL_TASKS = 1000


@pytest.mark.performance_tests
def test_bulk_request_too_large(db_cfg, client_env):
    """test that bulk bind can handle an extremely large bind request."""
    from jobmon.client.execution.strategies.multiprocess import MultiprocessExecutor
    from jobmon.client.api import Tool, BashTask
    num_tasks = 50
    large_cmd = "--abc ajfkadsljaskdfjadssdfhawe --bcd njdslgerdgfgbdfasfasdfasdfadssfadsfaf" \
                " --cdf fhadsjfklerngdfsfsdas --def fdbsjfserrjdfsadfasdfdsfjasd;fjaklsdfjak" \
                " --efg fajdskflanrefdsfadsfdsfajksdfkal --fgh afedhferfdsbjagefdbjasdfhaiwe" \
                " --ghi fbasdjkflanjekfdsjlefbdsfwejo --hij aenfdsjkxlaefndsergfdedsfagdffdf" \
                " --ijk fbdsjklanefdlefdfasdfaefsdffdf --jkl fdsjklnefafdsfffjwklsdnegrfdsef" \
                " --klm baejlsdfhjelnfsdlefdasfsdfsfda --lmn absdufilneafuidsneuifdsadsfafdf" \
                " --mno bufrefdhueidsfbcewdsifueridsuwrefiop --nop fbdslfbeuiasdfasdfadsadse" \
                " --opq nfasdakjafadsgewrrrwredfsbnkfsbgnfs --pqr fbdkjasnbfnlnknkdlaewdssdf" \
                "--qrs dnvjgsegrmdfkasdafsdasadsfewaef --rst fbasdjflanewjfasdfnjfasdfjwefjd" \
                " --stu ajsfdngiwerudhietuowefuidew --tuv fhwueehuiewfuwieawefjaeeuwioreoiur" \
                " --uvw hwueifhgbeirfiwbeifeafawerfredr --vwx fjdaskfsjdfasdfewrkasdfadsfdas" \
                " --xyz fjkdsalfjsdafjdskfadsfkads --yza jfaksdlfjdaslfdsajfdsjfkadsfafdewfg" \
                " --zab fhdsjkfjdsklfadsjkfasdjfjdskl" \
                "--abc ajfkadsljaskdfjadssdfhawe --bcd njdslgerdgfgbdfasfasdfasdfadssfadsfaf" \
                " --cdf fhadsjfklerngdfsfsdas --def fdbsjfserrjdfsadfasdfdsfjasd;fjaklsdfjak" \
                " --efg fajdskflanrefdsfadsfdsfajksdfkal --fgh afedhferfdsbjagefdbjasdfhaiwe" \
                " --ghi fbasdjkflanjekfdsjlefbdsfwejo --hij aenfdsjkxlaefndsergfdedsfagdffdf" \
                " --ijk fbdsjklanefdlefdfasdfaefsdffdf --jkl fdsjklnefafdsfffjwklsdnegrfdsef" \
                " --klm baejlsdfhjelnfsdlefdasfsdfsfda --lmn absdufilneafuidsneuifdsadsfafdf" \
                " --mno bufrefdhueidsfbcewdsifueridsuwrefiop --nop fbdslfbeuiasdfasdfadsadse" \
                " --opq nfasdakjafadsgewrrrwredfsbnkfsbgnfs --pqr fbdkjasnbfnlnknkdlaewdssdf" \
                "--qrs dnvjgsegrmdfkasdafsdasadsfewaef --rst fbasdjflanewjfasdfnjfasdfjwefjd" \
                " --stu ajsfdngiwerudhietuowefuidew --tuv fhwueehuiewfuwieawefjaeeuwioreoiur" \
                " --uvw hwueifhgbeirfiwbeifeafawerfredr --vwx fjdaskfsjdfasdfewrkasdfadsfdas" \
                " --xyz fjkdsalfjsdafjdskfadsfkads --yza jfaksdlfjdaslfdsajfdsjfkadsfafdewfg" \
                " --zab fhdsjkfjdsklfadsjkfasdjfjdskl" \
                "--abc ajfkadsljaskdfjadssdfhawe --bcd njdslgerdgfgbdfasfasdfasdfadssfadsfaf" \
                " --cdf fhadsjfklerngdfsfsdas --def fdbsjfserrjdfsadfasdfdsfjasd;fjaklsdfjak" \
                " --efg fajdskflanrefdsfadsfdsfajksdfkal --fgh afedhferfdsbjagefdbjasdfhaiwe" \
                " --ghi fbasdjkflanjekfdsjlefbdsfwejo --hij aenfdsjkxlaefndsergfdedsfagdffdf" \
                " --ijk fbdsjklanefdlefdfasdfaefsdffdf --jkl fdsjklnefafdsfffjwklsdnegrfdsef" \
                " --klm baejlsdfhjelnfsdlefdasfsdfsfda --lmn absdufilneafuidsneuifdsadsfafdf" \
                " --mno bufrefdhueidsfbcewdsifueridsuwrefiop --nop fbdslfbeuiasdfasdfadsadse" \
                " --opq nfasdakjafadsgewrrrwredfsbnkfsbgnfs --pqr fbdkjasnbfnlnknkdlaewdssdf" \
                "--qrs dnvjgsegrmdfkasdafsdasadsfewaef --rst fbasdjflanewjfasdfnjfasdfjwefjd" \
                " --stu ajsfdngiwerudhietuowefuidew --tuv fhwueehuiewfuwieawefjaeeuwioreoiur" \
                " --uvw hwueifhgbeirfiwbeifeafawerfredr --vwx fjdaskfsjdfasdfewrkasdfadsfdas" \
                " --xyz fjkdsalfjsdafjdskfadsfkads --yza jfaksdlfjdaslfdsajfdsjfkadsfafdewfg" \
                " --zab fhdsjkfjdsklfadsjkfasdjfjdskl"  # 2808 characters

    tasks = []
    for i in range(num_tasks):
        task = BashTask(command=f"sleep {i}; echo {large_cmd}", num_cores=1)
        tasks.append(task)
    unknown_tool = Tool()
    workflow = unknown_tool.create_workflow(name="bulk_bind_too_big", chunk_size=num_tasks)
    workflow.set_executor(MultiprocessExecutor(parallelism=3))
    workflow.add_tasks(tasks)
    # nodes
    workflow._bind()
    # tasks
    workflow._create_workflow_run()
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        sql = """
            SELECT *
            FROM task
            WHERE task.workflow_id = :workflow_id"""
        res = DB.session.execute(sql, {"workflow_id": workflow.workflow_id}).fetchall()
        DB.session.commit()
    assert len(res) == num_tasks


@pytest.mark.performance_tests
def test_one_by_one(db_cfg, client_env):
    requester = Requester(client_env, logger)

    for i in range(0, TOTAL_TASKS):
        tasks = []
        task = {}
        task["workflow_id"] = 1
        task["node_id"] = 1
        task["task_args_hash"] = i
        task["name"] = f"name{i}"
        task["command"] = "whatever"
        task["max_attempts"] = 1
        task["task_args"] = {}
        task["task_attributes"] = None
        tasks.append(task)
        rc, _ = requester.send_request(
            app_route='/task',
            message={'tasks': tasks},
            request_type='post')
        assert rc == 200


@pytest.mark.performance_tests
def test_bulk(db_cfg, client_env):
    requester = Requester(client_env, logger)
    tasks = []
    for i in range(0, TOTAL_TASKS):
        task = {}
        task["workflow_id"] = 2
        task["node_id"] = 2
        task["task_args_hash"] = i
        task["task_args"] = {}
        task["name"] = f"name{i}"
        task["command"] = "whatever"
        task["max_attempts"] = 1
        task["task_attributes"] = None
        tasks.append(task)
    rc, _ = requester.send_request(
        app_route='/task',
        message={'tasks': tasks},
        request_type='post')
    assert rc == 200
