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
    from jobmon.client.swarm.workflow import Workflow
    from jobmon.client.swarm.python_task import PythonTask

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

            self.workflow = Workflow(
                workflow_args='burdenator_v{v}'.format(v=self.version),
                name='burdenator', project='proj_burdenator')
            self.most_detailed_jobs_by_command = {}
            self.pct_change_jobs_by_command = {}
            self.loc_agg_jobs_by_command = {}
            self.cleanup_jobs_by_command = {}

        def create_most_detailed_jobs(self):
            """First set of tasks, thus no upstream tasks"""
            for loc in self.most_detailed_location_ids:
                for year in self.year_ids:
                    task = PythonTask(script='run_burdenator_most_detailed',
                                      args=[loc, year],
                                      name='most_detailed_{}_{}'.format(loc, year),
                                      num_cores=40, m_mem_free=20, max_attempts=5,
                                      max_runtime=360)
                    self.workflow.add_task(task)
                    self.most_detailed_jobs_by_command[task.name] = task

        def create_loc_agg_jobs(self):
            """Depends on most detailed jobs"""
            for year in self.year_ids:
                for sex in self.sex_ids:
                    for measure in self.measure_ids:
                        for rei in self.rei_ids:
                            task = PythonTask(
                                script='run_loc_agg',
                                args=[measure, year, sex, rei],
                                name='loc_agg_{}_{}_{}_{}'.format(measure, year, sex, rei),
                                num_cores=20, m_mem_free=40, max_runtime=540,
                                max_attempts=11)
                            for loc in self.most_detailed_location_ids:
                                task.add_upstream(
                                    self.most_detailed_jobs_by_command['most_detailed_{}_{}'
                                                                       .format(loc, year)])
                            self.workflow.add_task(task)
                            self.loc_agg_jobs_by_command[task.name] = task

        def create_cleanup_jobs(self):
            """Depends on aggregate locations coming out of loc agg jobs"""
            for measure in self.measure_ids:
                for loc in self.aggregate_location_ids:
                    for year in self.year_ids:
                        task = PythonTask(script='run_cleanup', args=[measure, loc, year],
                                          name='cleanup_{}_{}_{}'.format(measure, loc, year),
                                          num_cores=25, m_mem_free=50, max_runtime=360,
                                          max_attempts=11)
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
            for measure in self.measure_ids:
                for start_year, end_year in zip(self.start_year_ids, self.end_year_ids):
                    for loc in self.location_ids:
                        if loc in self.aggregate_location_ids:
                            is_aggregate = True
                        else:
                            is_aggregate = False
                        task = PythonTask(script='run_pct_change', args=[measure, loc,
                                                                         start_year,
                                                                         end_year],
                                          name=('pct_change_{}_{}_{}_{}'
                                                .format(measure, loc, start_year, end_year),
                                          num_cores=45, m_mem_free=90, max_attempts=11,
                                          max_runtime=540)
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
            for measure in self.measure_ids:
                task = PythonTask(script='run_pct_change', args=[measure],
                                  name='upload_{}'.format(measure), num_cores=20, m_mem_free=40,
                                  max_runtime=720, max_attempts=3)
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



