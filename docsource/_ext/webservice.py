import os
from docutils import nodes
from docutils.parsers.rst import Directive


class WebService(Directive):

    def run(self):
        webservice_str = "jobmon update_config --web_service_fqdn " + \
                         os.environ.get("WEB_SERVICE_FQDN") + \
                         " --web_service_fqdn " + \
                         os.environ.get("WEB_SERVICE_PORT")
        paragraph_node = nodes.paragraph(text=webservice_str)
        return [paragraph_node]

def setup(app):
    app.add_directive("webservicedir", WebService)

    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
