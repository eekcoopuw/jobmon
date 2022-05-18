"""Initialize Web services."""
from jobmon.server.web.start import JobmonServer

app = JobmonServer().create_app()
