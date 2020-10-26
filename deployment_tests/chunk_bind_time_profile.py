import time
from jobmon.client.workflow import Workflow
from jobmon.client.templates.bash_task import BashTask


tasks = [BashTask(f'echo {x}') for x in range(50)]

def profile(tasks, chunk_size: int = None):

	times = []

	for _ in range(10):
		if chunk_size:
			wf = Workflow(1, chunk_size=chunk_size)
		else:
			wf = Workflow(1)
		wf.add_tasks(tasks)
		start = time.time()
		wf._bulk_bind_nodes()
		end = time.time()
		times.append(end-start)

	print(f"Binding took {sum(times) / len(times)}s on average with a chunk size of {chunk_size}")

profile(tasks, 1)
profile(tasks, 10)
profile(tasks, 50000)
