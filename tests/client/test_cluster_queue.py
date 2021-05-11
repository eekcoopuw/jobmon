from jobmon.serializers import SerializeQueue, SerializeCluster


def test_cluster_queue(db_cfg, client_env):
    """test cluster_type, cluster and queue structure"""

    # Manually modify the database so that some cluster_type, cluster and queue
    # data could be tested
    from jobmon.client.api import BashTask, Tool
    from jobmon.client.execution.strategies.sequential import \
        SequentialExecutor

    # setup workflow 1
    tool = Tool()
    workflow1 = tool.create_workflow(name="test_cluster")
    executor = SequentialExecutor()
    workflow1.set_executor(executor)
    task_a = BashTask("sleep 5", executor_class="SequentialExecutor",
                      max_attempts=3)
    workflow1.add_task(task_a)

    # add workflow to database
    workflow1.bind()
    wfr_1 = workflow1._create_workflow_run()

    # now set everything to error fail
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        # fake a new cluster_type zzzzCLUSTER_TYPE
        DB.session.execute("""
            INSERT INTO cluster_type (name)
            VALUES ('{n}')""".format(n="zzzzCLUSTER_TYPE"))
        DB.session.commit()
        # fake 3 new cluster zzzzCLUSTER1, zzzzCLUSTER2, and zzzz我的新集群3 under zzzzCLUSTER_TYPE;
        # and to test out Unicode charset briefly.
        DB.session.execute("""
            INSERT INTO cluster (name, cluster_type_id)
            SELECT '{n}', id
            FROM cluster_type
            WHERE name = '{ct_name}'""".format(n="zzzzCLUSTER1", ct_name="zzzzCLUSTER_TYPE"))
        DB.session.execute("""
            INSERT INTO cluster (name, cluster_type_id)
            SELECT '{n}', id
            FROM cluster_type
            WHERE name = '{ct_name}'""".format(n="zzzzCLUSTER2", ct_name="zzzzCLUSTER_TYPE"))
        DB.session.execute("""
            INSERT INTO cluster (name, cluster_type_id)
            SELECT '{n}', id
            FROM cluster_type
            WHERE name = '{ct_name}'""".format(n="zzzz我的新集群3", ct_name="zzzzCLUSTER_TYPE"))
        DB.session.commit()

        # fake 2 new queues for zzzzCluster2
        DB.session.execute("""
            INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
            SELECT 'all.q', c.id, 'cust param 1' AS `parameters`
            FROM cluster c
            WHERE c.name = '{n}'""".format(n="zzzzCLUSTER2"))
        DB.session.execute("""
            INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
            SELECT 'long.q', c.id, 'cust param 2' AS `parameters`
            FROM cluster c
            WHERE c.name = '{n}'""".format(n="zzzzCLUSTER2"))
        DB.session.commit()

    # make sure that the 3 clusters logged above are among the all_clusters
    rc, response = workflow1.requester.send_request(
        app_route=f'/client/all_clusters',
        message={},
        request_type='get')
    all_clusters = [
        SerializeCluster.kwargs_from_wire(j)
            for j in response['clusters']]
    target_clusters = [j for j in all_clusters if j["cluster_type_name"] == "zzzzCLUSTER_TYPE"]
    assert len(target_clusters) == 3

    # make sure that a single pull of one of the 3 clusters logged above gets 1 record back.
    rc, response = workflow1.requester.send_request(
        app_route=f'/client/cluster/zzzzCLUSTER2',
        message={},
        request_type='get')
    cl = SerializeCluster.kwargs_from_wire(response['cluster'])
    assert cl["cluster_type_name"] == "zzzzCLUSTER_TYPE"

    # make sure that the 2 queues logged above are among the all_clusters for the concerned cluster
    rc, response = workflow1.requester.send_request(
        app_route=f'/client/cluster/zzzzCLUSTER2/all_queues',
        message={},
        request_type='get')
    all_queues = [
        SerializeQueue.kwargs_from_wire(j)
            for j in response['queues']]
    assert len(all_queues) == 2

    # make sure that a single pull of one of the 2 queues logged above gets 1 record back.
    rc, response = workflow1.requester.send_request(
        app_route=f'/client/cluster/zzzzCLUSTER2/queue/all.q',
        message={},
        request_type='get')
    cl = SerializeQueue.kwargs_from_wire(response['queue'])
    assert cl["cluster_type_name"] == "zzzzCLUSTER_TYPE"
