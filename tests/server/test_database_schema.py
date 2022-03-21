def test_arg_name_collation(db_cfg, ephemera):
    """test both lowercase and uppercase have no conflict."""
    app = db_cfg["app"]
    DB = db_cfg["DB"]
    with app.app_context():
        result = DB.session.execute(
            """
            INSERT INTO arg(name)
            VALUES
                ('r'),
                ('R'),
                ('test_case'),
                ('TEST_CASE');
            """
        )
        DB.session.commit()
        assert result.rowcount == 4
