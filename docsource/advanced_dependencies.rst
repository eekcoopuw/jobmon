Example for How to Use Jobmon for Advanced Dependencies
*******************************************************

For this example, we'll use a slighly simplified version of the Burdenator which has five
"phases": most-detailed, pct-change, loc-agg, cleanup, and upload. To reduce runtime,
we want to link up each job only to the previous jobs that it requires, not to every job
in that phase. The parallelization strategies for each phase are a little different,
complicating the dependency scheme.

1. Most-detailed jobs are parallelized by location, year;
2. Loc-agg jobs are parallelized by measure, year, rei, and sex;
3. Cleanup jobs are parallelized by location, measure, year
4. Pct-change jobs are parallelized by location_id, measure, start_year, end_year; For most-detailed locations, this can run immediately after the most-detailed phase. But for aggregate locations, this has to be run after both loc-agg and cleanup
5. Upload jobs are parallelized by measure

To begin, we create an empty dictionary for each phase and when we build each task, we add the
task to its dictionary. Then the task in the following phase can find its upstream task using
the upstream dictionary. The only dictionary not needed is one for the upload jobs, since no
downstream tasks depend on these jobs.

.. code::

    # python 3
    import sys
    from jobmon.client.tool import Tool
    from jobmon.client.task_template import TaskTemplate
    from jobmon.client.execution.strategies.base import ExecutorParameters

    from my_app.utils import split_locs_by_loc_set

    class NatorJobSwarm(object):
        def __init__(self, year_ids, start_years, end_years, location_set_id,
                     measure_ids, rei_ids, sex_ids, version):
            self.year_ids = year_ids
            self.start_year_ids = start_years
            self.end_year_ids = end_years
            self.most_detailed_location_ids, self.aggregate_location_ids, \
                self.all_location_ids = split_locs_by_loc_set(location_set_id)
            self.measure_ids = measure_ids
            self.rei_ids = rei_ids
            self.sex_ids = sex_ids
            self.version = version

            self.tool = Tool(name="Burdenator")
            self.most_detailed_jobs_by_command = {}
            self.pct_change_jobs_by_command = {}
            self.loc_agg_jobs_by_command = {}
            self.cleanup_jobs_by_command = {}

            self.python = sys.executable

        def create_workflow(self):
            """ Instantiate the workflow """

            self.workflow = self.tool.create_workflow(
                workflow_args = f'burdenator_v{self.version}',
                name = f'burdenator run {self.version}'
            )

        def create_task_templates(self):
            """ Create the task template metadata objects """

            self.most_detailed_tt = self.tool.get_task_template(
                template_name = "run_burdenator_most_detailed",
                command_template = "{python} {script} --location_id {location_id} --year {year}",
                node_args = ["location_id", "year"],
                op_args = ["python", "script"])

            self.loc_agg_tt = self.tool.get_task_template(
                template_name = "location_aggregation",
                command_template = "{python} {script} --measure {measure} --year {year} --sex {sex} --rei {rei}",
                node_args = ["measure", "year", "sex", "rei"],
                op_args = ["python", "script"])

            self.cleanup_jobs_tt = self.tool.get_task_template(
                template_name = "cleanup_jobs",
                command_template = "{python} {script} --measure {measure} --loc {loc} --year {year}",
                node_args = ["measure", "loc", "year"],
                op_args = ["python", "script"])

            self.pct_change_tt = self.tool.get_task_template(
                template_name = "pct_change",
                command_template = ("{python} {script} --measure {measure} --loc {loc} --start_year {start_year}"
                                    " --end_year {end_year}"),
                node_args = ["measure", "loc", "start_year", "end_year"],
                op_args = ["python", "script"])

            self.upload_tt = self.tool.get_task_template(
                template_name = "upload_jobs",
                command_template = "{python} {script} --measure {measure}"
                node_args = ["measure"],
                op_args = ["python", "script"])


        def create_most_detailed_jobs(self):
            """First set of tasks, thus no upstream tasks"""

            executor_parameters = ExecutorParameters(
                num_cores=40,
                m_mem_free="20G",
                max_attempts=5,
                max_runtime_seconds=360
            )

            for loc in self.most_detailed_location_ids:
                for year in self.year_ids:
                    task = self.most_detailed_tt.create_task(
                                      executor_parameters=executor_parameters,
                                      name='most_detailed_{}_{}'.format(loc, year),
                                      python=self.python,
                                      script='run_burdenator_most_detailed',
                                      loc=loc,
                                      year=year)
                    self.workflow.add_task(task)
                    self.most_detailed_jobs_by_command[task.name] = task

        def create_loc_agg_jobs(self):
            """Depends on most detailed jobs"""

            executor_parameters = ExecutorParameters(
                num_cores=20,
                m_mem_free="40G",
                max_attempts=11,
                max_runtime_seconds=540
            )

            for year in self.year_ids:
                for sex in self.sex_ids:
                    for measure in self.measure_ids:
                        for rei in self.rei_ids:
                            task = self.loc_agg_tt.create_task(
                                executor_parameters=executor_parameters,
                                name='loc_agg_{}_{}_{}_{}'.format(measure, year, sex, rei),
                                python=self.python,
                                script='run_loc_agg',
                                measure=measure,
                                year=year,
                                sex=sex,
                                rei=rei)

                            for loc in self.most_detailed_location_ids:
                                task.add_upstream(
                                    self.most_detailed_jobs_by_command['most_detailed_{}_{}'
                                                                       .format(loc, year)])
                            self.workflow.add_task(task)
                            self.loc_agg_jobs_by_command[task.name] = task

        def create_cleanup_jobs(self):
            """Depends on aggregate locations coming out of loc agg jobs"""

            executor_parameters = ExecutorParameters(
                num_cores=25,
                m_mem_free="50G",
                max_attempts=11,
                max_runtime_seconds=360
            )

            for measure in self.measure_ids:
                for loc in self.aggregate_location_ids:
                    for year in self.year_ids:
                        task = self.cleanup_jobs_tt.create_task(
                                          executor_parameters=executor_parameters,
                                          name='cleanup_{}_{}_{}'.format(measure, loc, year),
                                          python=self.python,
                                          script='run_cleanup',
                                          measure=measure,
                                          loc=loc,
                                          year=year)

                        for sex in self.sex_ids:
                            for rei in self.rei_ids:
                                task.add_upstream(
                                    self.loc_agg_jobs_by_command['loc_agg_{}_{}_{}_{}'
                                                                 .format(measure, year,
                                                                         sex, rei)])
                        self.workflow.add_task
                        self.cleanup_jobs_by_command[task.name] = task

        def create_pct_change_jobs(self):
            """For aggregate locations, depends on cleanup jobs.
            But for most_detailed locations, depends only on most_detailed jobs"""

            executor_parameters = ExecutorParameters(
                num_cores=45,
                m_mem_free="90G",
                max_attempts=11,
                max_runtime_seconds=540
            )

            for measure in self.measure_ids:
                for start_year, end_year in zip(self.start_year_ids, self.end_year_ids):
                    for loc in self.location_ids:
                        if loc in self.aggregate_location_ids:
                            is_aggregate = True
                        else:
                            is_aggregate = False
                        task = self.pct_change_tt.create_task(
                                          executor_parameters=executor_parameters,
                                          name=('pct_change_{}_{}_{}_{}'
                                                .format(measure, loc, start_year, end_year),
                                          python=self.python,
                                          script='run_pct_change',
                                          measure=measure,
                                          loc=loc,
                                          start_year=start_year,
                                          end_year=end_year)

                        for year in [start_year, end_year]:
                            if is_aggregate:
                                task.add_upstream(
                                    self.cleanup_jobs_by_command['cleanup_{}_{}_{}'
                                                                 .format(measure, loc, year)]
                            else:
                                task.add_upstream(
                                    self.most_detailed_jobs_by_command['most_detailed_{}_{}'
                                                                       .format(loc, year)])
                        self.workflow.add_task(task)
                        self.pct_change_jobs_by_command[task.name] = task

        def create_upload_jobs(self):
            """Depends on pct-change jobs"""

            executor_parameters = ExecutorParameters(
                num_cores=20,
                m_mem_free="40G",
                max_attempts=3,
                max_runtime_seconds=720
            )

            for measure in self.measure_ids:
                task = self.upload_tt.create_task(
                                  executor_parameters=executor_parameters,
                                  name='upload_{}'.format(measure)
                                  script='run_pct_change',
                                  measure=measure)

                for location_id in self.all_location_ids:
                    for start_year, end_year in zip(self.start_year_ids, self.end_year_ids):
                        task.add_upstream(
                            self.pct_change_jobs_by_command['pct_change_{}_{}_{}_{}'
                                                            .format(measure, location,
                                                                    start_year, end_year])
                self.workflow.add_task(task)

        def run():
            success = self.workflow.run()
            if success:
                print("You win at life")
            else:
                print("Failure")



