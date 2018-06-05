Dag Visualization
#################


Visualize your dag
******************

After you create your workflow, you may want to inspect its interdepencies via a visualization.
To do this is very simple. First you must have your workflow created and all tasks already added to it. Then do the following:

..code::

    from jobmon.workflow.task_dag_viz import TaskDagViz

    TaskDagViz(workflow.task_dag, graph_outdir='path/to/my/outdir',
               output_format='svg').render()

The resulting image will show up in the graph_outdir, with the filename matching the name of your dag.

Optionally you may specify options to TaskDagViz() to change the number of inches between nodes of different levels (ranksep), number of inches between nodes of the same level (nodesep), overall width of the graph (width), and over height of the graph (height).

..note::
    Warning: For very big workflows, TaskDagViz may take a very long time to run, because Graphviz, the library on top of which TaskDagViz is built, is documented to be very `slow to render <https://stackoverflow.com/questions/10766100/graphviz-dot-very-long-duration-of-generation>`_. We recommmend you create a smaller version of your whole workflow to visualize. For instance, if you normally run on 38 years, run the TaskDagViz on a Workflow of just 2 years. The resulting graph will be produced way faster and be far more legible.
