from jobmon.requester import Requester
from jobmon.serializers import SerializeQueue, SerializeCluster


def test_cluster_queue(db_cfg, client_env):
    """test cluster_type, cluster and queue structure"""
    requester = Requester(client_env)

    # now set everything to error fail
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        # fake a new cluster_type zzzzCLUSTER_TYPE
        DB.session.execute(
            """
            INSERT INTO cluster_type (name)
            VALUES ('{n}')""".format(
                n="zzzzCLUSTER_TYPE"
            )
        )
        DB.session.commit()
        # fake 3 new cluster zzzzCLUSTER1, zzzzCLUSTER2, and zzzz我的新集群3 under zzzzCLUSTER_TYPE;
        # and to test out Unicode charset briefly.
        DB.session.execute(
            """
            INSERT INTO cluster (name, cluster_type_id)
            SELECT '{n}', id
            FROM cluster_type
            WHERE name = '{ct_name}'""".format(
                n="zzzzCLUSTER1", ct_name="zzzzCLUSTER_TYPE"
            )
        )
        DB.session.execute(
            """
            INSERT INTO cluster (name, cluster_type_id)
            SELECT '{n}', id
            FROM cluster_type
            WHERE name = '{ct_name}'""".format(
                n="zzzzCLUSTER2", ct_name="zzzzCLUSTER_TYPE"
            )
        )
        DB.session.execute(
            """
            INSERT INTO cluster (name, cluster_type_id)
            SELECT '{n}', id
            FROM cluster_type
            WHERE name = '{ct_name}'""".format(
                n="zzzz我的新集群3", ct_name="zzzzCLUSTER_TYPE"
            )
        )
        DB.session.commit()

        # fake 2 new queues for zzzzCluster2
        DB.session.execute(
            """
            INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
            SELECT 'all.q', c.id, "{{'cust': 'param 1'}}" AS `parameters`
            FROM cluster c
            WHERE c.name = '{n}'""".format(
                n="zzzzCLUSTER2"
            )
        )
        DB.session.execute(
            """
            INSERT INTO `queue`(`name`, `cluster_id`, `parameters`)
            SELECT 'long.q', c.id, "{{'cust': 'param 2'}}" AS `parameters`
            FROM cluster c
            WHERE c.name = '{n}'""".format(
                n="zzzzCLUSTER2"
            )
        )
        DB.session.commit()

    # make sure that the 3 clusters logged above are among the all_clusters
    rc, response = requester.send_request(
        app_route="/all_clusters", message={}, request_type="get"
    )
    all_clusters = [SerializeCluster.kwargs_from_wire(j) for j in response["clusters"]]
    target_clusters = [
        j for j in all_clusters if j["cluster_type_name"] == "zzzzCLUSTER_TYPE"
    ]
    assert len(target_clusters) == 3

    # make sure that a single pull of one of the 3 clusters logged above gets 1 record back.
    rc, response = requester.send_request(
        app_route="/cluster/zzzzCLUSTER2", message={}, request_type="get"
    )
    cluster2 = SerializeCluster.kwargs_from_wire(response["cluster"])
    assert cluster2["cluster_type_name"] == "zzzzCLUSTER_TYPE"

    # make sure that the 2 queues logged above are among the all_clusters for the concerned cluster
    rc, response = requester.send_request(
        app_route="/cluster/zzzzCLUSTER2/all_queues", message={}, request_type="get"
    )
    all_queues = [SerializeQueue.kwargs_from_wire(j) for j in response["queues"]]
    assert len(all_queues) == 2

    # make sure that a single pull of one of the 2 queues logged above gets 1 record back.
    rc, response = requester.send_request(
        app_route=f'/cluster/{cluster2["id"]}/queue/all.q',
        message={},
        request_type="get",
    )
    all_q = SerializeQueue.kwargs_from_wire(response["queue"])
    assert all_q["parameters"]["cust"] == "param 1"
