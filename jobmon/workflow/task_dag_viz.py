import os

import graphviz as gv


class TaskDagViz(object):
    def __init__(self, task_dag, graph_outdir, output_format='pdf',):
        self.task_dag = task_dag
        self.graph_outdir = graph_outdir

        self.output_format = output_format
        self.graph = gv.Digraph(format=self.output_format)

    def render(self):
        for _, task in self.task_dag.tasks.items():
            self.graph.node(task.name)
            for upstream in task.upstream_tasks:
                self.graph.edge(upstream.name, task.name)
        self.graph.render(
            os.path.join(self.graph_outdir, '{}'.format(self.task_dag.name)))
