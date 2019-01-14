from jobmon.models.job_instance import JobInstance


def test_table_job_instance_index_executor_id(db_cfg):
    """Test index is created on executor_id in job_instance_table and it's not unique"""
    app = db_cfg["app"]
    DB = db_cfg["DB"]

    with app.app_context():
        for c in JobInstance.__table__.columns:
            if c.name.lower() == 'executor_id':
                assert c.index is True
                assert c.unique is None