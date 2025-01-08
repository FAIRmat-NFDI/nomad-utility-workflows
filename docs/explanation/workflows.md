# Custom Workflows

NOMAD contains an internal generic schema to represented workflows as directed graphs.
"Standard workflows" for simulations are implemented as python classes within NOMAD's `simulation-workflow-schema` plugin.
NOMAD also allows users to define their own procedures that connect NOMAD entries or generic tasks into "custom workflows".
To achieve this in practice, user must create a `workflow.archive.yaml` file that specifies these connections according to a pre-described format that NOMAD recognizes.
(Link to other docs, maybe copy over one simple example?)

While effective, the creation of this yaml file requires some a priori knowledge of NOMAD's simulation schema to appropriately connect different type of entries and input/output sections within the desired graph structure.

`nomad-utility-workflows` attempts to simplify the creation of the custom workflow yaml files by allowing users instead to supply a networkx graph with a set of minimal node attributes that are then used to create the appropriate connections within the yaml file automatically.

## NetworkX DiGraphs

A networkx directed graph is instantiated as follows:
```python
import networkx as nx
workflow_graph = nx.DiGraph()
```

see [NetworkX Docs > DiGraph](https://networkx.org/documentation/stable/reference/classes/digraph.html) for more information.

## Node Attributes

The following attributes can be added to each node in the graph:

```python
{
'name': str
    """
    a free-form string which describing this node,
    will be used as a label in the NOMAD workflow graph visualizer
    """,

'type': literal('input', 'output', 'workflow', 'task', 'other')
    """
    specifies the type of node. Must be one of the above-specified options.
    -----
    input: (meta)data taken as input for the entire workflow or a specific task.
    For simulations, often corresponds to a section within the archive (e.g., system, method)

    output: (meta)data produced as output for the entire workflow or a specific task.
    For simulations, often corresponds to a section within the archive (e.g., calculation)

    workflow: a node in the workflow which itself contains an internal (sub)workflow, that is
    recognized by NOMAD.
    Such nodes can be linked to existing workflows within NOMAD, providing functionalities
    within NOMAD's interactive workflow graphs.

    task: a node in the workflow which represents an individual task (i.e., no underlying workflow),
    that is recognized by NOMAD.

    other: a node in the workflow which represents either a (sub)workflow or individual task that
    is not supported by NOMAD.
    """,

'entry_type': literal('simulation')
    """
    specifies the type of node in terms of tasks or workflows recognized by NOMAD.
    Functionally, this attribute is used to create default inputs and outputs that are
    required for properly creating the edge visualizations in the NOMAD GUI.
    """,

'path_info': dict(
    """
    information for generating the NOMAD archive section paths
    (i.e., connections between nodes in terms of the NOMAD MetaInfo sections)
    """

    'upload_id': str
        """
        NOMAD PID for the upload, if exists
        """

    'entry_id': str
        """
        NOMAD PID for the entry, if exists
        """

    'mainfile_path': str
        """
        local (relative to the native upload) path to the mainfile,
        including the mainfile name with extension.
        """

    'supersection_path': str,
        """
        archive path to the supersection, e.g., "run" or "workflow2/method"
        """

    'supersection_index': int,
        """
        the relevant index for the supersection, if it is a repeating subsection
        """

    'section_type': str,
        """
        the name of the section for an input or output node,
        e.g., "system", "method", or "calcuation"
        """

    'section_index': int,
        """
        the relevant index for the section, if it is a repeating sebsection
        """

    'archive_path': str
        """
        specifies the entire archive path to the section,
        e.g., "run/0/system/2"
        """
)

'inputs': list(dict)
"""
a list of input nodes to be added to the graph with in_edges to the parent node.
"""
[
    {
        'name': str
            """
            will be set as the name for the input node created
            """
        'path_info': dict()
            """
            path information for the input node created,
            as specified for the node attributes above.
            """
    }
],
'outputs': list(dict)
"""
a list of output nodes to be added to the graph with out_edges from the parent node.
"""
[
    {
        'name': str
            """
            will be set as the name for the output node created
            """
        'path_info': dict()
            """
            path information for the output node created,
            as specified for the node attributes above.
            """
    }
],
}
```

This can be achieved in practice with:

```python
node_attributes = {
    0: {'<key, value pairs as defined above>'},
    1: {'<key, value pairs as defined above>'},
    ...
}
```

1. For a dictionary containing all nodes and attributes

```python
workflow_graph.add_nodes_from(node_attributes.keys())
nx.set_node_attributes(workflow_graph, node_attributes)
```

2. For each node attribute dictionary separately:

```python
for node_index, node_attribute_dict in node_attributes.items():
    workflow_graph.add_node(node_index, **node_attribute_dict)
```

Then, the appropriate edges need to be added to the graph (no edge attributes are necessary). For each source to destination edge:

```python
workflow_graph.add_edge(
    node_source, node_destination
)
```

## Generating the initial workflow graph

Alternatively, `nomad-utility-workflows` provides a functionality to automatically create an initial workflow graph automatically from a dictionary of node attributes as defined above with the function `node_to_attributes()`. In this case, the edges are specified with the following additional attributes (duplicate edges do not have an effect):

```python
{
'in_edge_nodes': list(int)
    """
    a list of integers specifying the node keys which contain in-edges to this node.
    """

'out_edge_nodes': list(int)
    """
    a list of integers specifying the node keys which contain out-edges to this node.
    """
}
```