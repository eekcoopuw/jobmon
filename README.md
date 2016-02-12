# Job Monitor
The package intends to provide simple, central monitoring of statuses and errors encountered by distributed tasks. It seeks to easily drop-in to existing code bases without significant refactoring.

Login to separate nodes and try out the client/server code below. A sqlite file named *job_monitor.sqlite* should show up in ~ (or whatever directory you specify) containing a log of status updates and errors.

## Server node
```python
import monitor
m = monitor.JobMonitor("~")
m.run()
```

## Client node(s)
```python
import job
import time
j1 = job.Job("~", jid=12345)
j1.start()
time.sleep(5)
j1.finish()
```
```python
import job
import time
j2 = job.Job("~", jid=98765)
j2.start()
time.sleep(2)
j2.log_error('Uh oh! Something went wrong!')
j2.failed()
```

## Dependencies
- pyzmq
