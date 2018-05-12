import os

import graphviz as gv


class TaskDagViz(object):
    def __init__(self, task_dag, graph_outdir, output_format='svg',
                 ranksep=5.0, nodesep=0.5, width=30, height=100):
        """
        task_dag (TaskDag obj): jobmon TaskDag object with all tasks added
        graph_outdir (str): filepath to an existing directory where graph
            should be output
        output_format (str, default='svg'): format in which the graph should be
            output
        ranksep (float, default=5.0): desired rank separation in inches of the
            nodes in the graph, by rank or level
        nodesep (float, default=0.5): desired node separation in inches of the
            nodes in the graph
        width (int, default=30): desired width of drawing in inches
        height (int, default=100): desired height of drawing in inches
        """
        self.task_dag = task_dag
        self.graph_outdir = graph_outdir

        self.output_format = output_format
        self.graph = gv.Digraph(format=self.output_format,
                                graph_attr={'nodesep': str(nodesep),
                                            'ranksep': str(ranksep),
                                            'size': '{}, {}'
                                            .format(width, height)})
        self.color_map = self.get_colors()

    def render(self):
        for _, task in self.task_dag.tasks.items():
            color = self.color_map.get(task.tag, 'ghostwhite')
            self.graph.node(task.name, "", fillcolor=color, style='filled')
            for upstream in task.upstream_tasks:
                self.graph.edge(upstream.name, task.name)
        self.graph.render(
            os.path.join(self.graph_outdir, '{}'.format(self.task_dag.name)))

    def get_colors(self):
        color_map = {}
        for i, tag in enumerate(self.task_dag.tags):
            # ring buffer the color spectrum
            color_map[tag] = "/set312/" + str((i + 1) % 12)
        return color_map

