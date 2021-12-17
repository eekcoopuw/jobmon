import pytest
from unittest.mock import patch

from jobmon.server.qpid_integration.squid_integrator import SlurmClusters

def test_SlurmClusters(db_cfg, ephemera):
    def fake_config():
        conn_str = "mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(
            user=ephemera["DB_USER"],
            pw=ephemera["DB_PASS"],
            host=ephemera["DB_HOST"],
            port=ephemera["DB_PORT"],
            db=ephemera["DB_NAME"],
        )
        print(f"*************************{conn_str}")
        return {"conn_str": conn_str,
            "polling_interval": 10,
            "max_update_per_sec": 10}

    with patch("jobmon.server.qpid_integration.squid_integrator._get_config", fake_config):
        SlurmClusters._cluster_type_name = "dummy"
        app = db_cfg["app"]
        DB = db_cfg["DB"]
        with app.app_context():
            sql = 'SELECT id FROM cluster_type WHERE name = \"dummy\"'
            ctid = DB.session.execute(sql).fetchone()["id"]
            i1 = SlurmClusters.get_instance(DB.session)
            assert i1.cluster_type_id == ctid
            sql = f"SELECT id FROM cluster WHERE cluster_type_id={ctid}"
            rows = DB.session.execute(sql)
            cids = [int(r["id"]) for r in rows]
            i2 = SlurmClusters.get_instance(DB.session)
            assert cids == i2.cluster_ids
            assert i1 is i2
