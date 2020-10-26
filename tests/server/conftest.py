import pytest


@pytest.fixture(scope='session')
def test_app(ephemera):
    """This sets up the JSM/JQS using the test_client which is a
    fake server
    """
    from jobmon.server import create_app
    from jobmon.server.server_config import ServerConfig

    # The create_app call sets up database connections
    server_config = ServerConfig(
        db_host=ephemera["DB_HOST"],
        db_port=ephemera["DB_PORT"],
        db_user=ephemera["DB_USER"],
        db_pass=ephemera["DB_PASS"],
        db_name=ephemera["DB_NAME"],
        wf_slack_channel=None,
        slack_token=None)
    app = create_app(server_config)
    app.config['TESTING'] = True
    client = app.test_client()
    yield client, client


def get_flask_content(response):
    """The function called by the no_request_jsm_jqs to query the fake
    test_client for a response
    """
    if 'application/json' in response.headers.get('Content-Type'):
        content = response.json
    elif 'text/html' in response.headers.get('Content-Type'):
        content = response.data
    else:
        content = response.content
    return response.status_code, content


@pytest.fixture(scope='function')
def in_memory_jsm_jqs(monkeypatch, test_app):
    """This function monkeypatches the requests library to use the
    test_client
    """
    import requests
    from jobmon.client import requester
    jsm_client, jqs_client = test_app

    def get_jqs(url, params, headers):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return jqs_client.get(path=url, query_string=params, headers=headers)
    monkeypatch.setattr(requests, 'get', get_jqs)
    monkeypatch.setattr(requester, 'get_content', get_flask_content)

    def post_jsm(url, json, headers):
        url = "/" + url.split(":")[-1].split("/", 1)[1]
        return jsm_client.post(url, json=json, headers=headers)
    monkeypatch.setattr(requests, 'post', post_jsm)
    monkeypatch.setattr(requester, 'get_content', get_flask_content)
