from jobmon.models.job_instance import JobInstance


def test_table_job_instance_index_executor_id(db_cfg):
    """Test index is created on executor_id in job_instance_table and it's not
       unique"""
    app = db_cfg["app"]
    with app.app_context():
        for c in JobInstance.__table__.columns:
            if c.name.lower() == 'executor_id':
                assert c.index is True
                assert c.unique is None


def test_dag_id_column_in_job_instance(db_cfg):
    """Test the db schema change of GBDSCI-1564"""
    app = db_cfg["app"]
    column_found = False
    with app.app_context():
        for c in JobInstance.__table__.columns:
            if c.name.lower() == 'dag_id':
                column_found = True
                break
    assert column_found


def test_dag_id_column_index_in_job_instance(db_cfg):
    """Test the db schema change of GBDSCI-1564"""
    app = db_cfg["app"]
    with app.app_context():
        for c in JobInstance.__table__.columns:
            if c.name.lower() == 'dag_id':
                assert c.index is True


def test_maxpss_in_job_instance(db_cfg):
    """Test the db schema change of GBDSCI-2313"""
    app = db_cfg["app"]
    column_found = False
    with app.app_context():
        for c in JobInstance.__table__.columns:
            if c.name.lower() == 'maxpss':
                column_found = True
                break
    assert column_found
