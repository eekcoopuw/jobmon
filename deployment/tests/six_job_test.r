library(argparse)
parser <- ArgumentParser()
parser$add_argument("--python-path",
                    help = "Path to python executable")
parser$add_argument("--jobmonr-loc",
                    help = "The location of jobmonr")

args <- parser$parse_args()
python_path <- args$python_path
jobmonr_loc <- args$jobmonr_loc

Sys.setenv("RETICULATE_PYTHON" = python_path)  # Set the Python interpreter path

library(devtools)
# install from source an in-memory package
devtools::load_all(path=jobmonr_loc)


# Bind a workflow to the tool
my_tool <- tool()
wf <- workflow(tool=my_tool, name=paste0("six-job-test-", runif(1)))

# Set the executor
wf <- set_executor(wf, executor_class='SGEExecutor', project="ihme_general")

# Define executor parameters for our tasks
params <- executor_parameters(num_cores=1, m_mem_free="1G", queue='all.q', max_runtime_seconds=100)

# Define task template
sleep_template <- task_template(tool=my_tool,
                                template_name='sleep_template',
                                command_template='sleep {sleep_time}',
                                node_args=list('sleep_time'))


# Create tasks
t1 <- task(task_template=sleep_template,
           executor_parameters=params,
           name='sleep_1',
           sleep_time="1")

# Second Tier, both depend on first tier
t2 <- task(task_template=sleep_template,
           executor_parameters=params,
           name='sleep_2',
           sleep_time="20",
           upstream_tasks = list(t1))
t3 <- task(task_template=sleep_template,
           executor_parameters=params,
           name='sleep_3',
           sleep_time="25",
           upstream_tasks = list(t1))

# Third Tier, cross product dependency on second tier
t4 <- task(task_template=sleep_template,
           executor_parameters=params,
           name='sleep_4',
           sleep_time="10",
           upstream_tasks = list(t2, t3))
t5 <- task(task_template=sleep_template,
           executor_parameters=params,
           name='sleep_5',
           sleep_time="13",
           upstream_tasks = list(t2, t3))

# Fourth Tier, ties it all back together
t6 <- task(task_template=sleep_template,
           executor_parameters=params,
           name='sleep_6',
           sleep_time="19",
           upstream_tasks = list(t4, t5))


# Add tasks to the workflow
wf <- add_tasks(wf, list(t1, t2, t3, t4, t5, t6))

# Run it
wfr <- run(workflow=wf, resume=F, seconds_until_timeout=600)
