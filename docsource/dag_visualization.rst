Dag Visualization
#################


Visualize your dag
******************

After you create your dag, you may want to inspect its interdepencies via a visualization.
To do this is very simple. First you must have your dag created and all tasks already added to it. Then do the following:

..code::

    from jobmon.workflow.task_dag_viz import TaskDagViz

    TaskDagViz(task_dag, graph_outdir='path/to/my/outdir',
               output_format='svg').render()

The resulting image will show up in the graph_outdir, with the filename matching the name of your dag.

Optionally you may specify options to TaskDagViz() to change the number of inches between nodes of different levels (ranksep), number of inches between nodes of the same level (nodesep), overall width of the graph (width), and over height of the graph (height).
