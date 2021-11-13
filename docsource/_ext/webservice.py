import os
from docutils import nodes
from docutils.parsers.rst import Directive


class WebServiceFqdn(Directive):

    def run(self):
        paragraph_node = nodes.paragraph(text=os.environ.get("WEB_SERVICE_FQDN"))
        return [paragraph_node]

class WebServicePort(Directive):

    def run(self):
        paragraph_node = nodes.paragraph(text=os.environ.get("WEB_SERVICE_PORT"))
        return [paragraph_node]

def setup(app):
    app.add_directive("webservicefqdn", WebServiceFqdn)
    app.add_directive("webserviceport", WebServicePort)

    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
