"""Initialize Web services."""
from jobmon.server.web.start import JobmonAppFactory

app = JobmonAppFactory().create_app_context()
