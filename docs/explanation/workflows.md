# Custom Workflows

NOMAD contains an internal generic schema to represented workflows as directed graphs.
"Standard workflows" for simulations are implemented as python classes within NOMAD's `simulation-workflow-schema` plugin.
NOMAD also allows users to define their own procedures that connect NOMAD entries or generic tasks into "custom workflows".
To achieve this in practice, user must create a `workflow.archive.yaml` file that specifies these connections according to a pre-described format that NOMAD recognizes.
(Link to other docs, maybe copy over one simple example?)

While effective, the creation of this yaml file requires some a priori knowledge of NOMAD's simulation schema to appropriately connect different type of entries and input/output sections within the desired graph structure.

`nomad-utility-workflows` attempts to simplify the creation of the custom workflow yaml files by allowing users instead to supply a networkx graph with a set of minimal node attributes that are then used to create the appropriate connections within the yaml file automatically.

networkx directed graph:
```python
import networkx as nx
workflow_graph = nx.DiGraph
```


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
```

`node_to_attributes()`:

```python
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



node_attributes = {
    0: {
        'name': 'global workflow input',
        'type': 'input',
        'entry_type': 'simulation',
        'path_info': {
            'upload_id': '<input_upload_id>',
            'entry_id': None,
            'mainfile_path': '<input_mainfile>',
            # ! Global inputs must reference the mainfile explicitly (i.e., not inferred
            # from the edge node)
            'supersection_index': 0,
            'section_index': 0,
            'section_type': 'method',
            # 'archive_path': 'run/0/method/0',
        },
        'out_edge_nodes': [1],
    },
    1: {
        'name': '1',
        'type': 'task',
        'entry_type': 'simulation',
        'path_info': {
            'upload_id': None,
            'entry_id': '<task_1_entry_id>',
            'mainfile_path': '<task_1_mainfile>',
            'section_type': 'workflow2',
            # 'archive_path': 'workflow2',
        },
        'inputs': [
            {
                'name': 'input system from 0',
                'path_info': {
                    'section_type': 'system',
                    'supersection_index': 0,
                    'section_index': 0,
                    # 'archive_path': 'run/0/system/0'
                },
            }
        ],
        'outputs': [
            {
                'name': 'output calculation from 1',
                'path_info': {
                    'section_type': 'calculation',
                    'supersection_index': 0,
                    'calculation_index': -1,
                    # 'archive_path': 'run/0/calculation/-1',
                },
            }
        ],
        # TODO In case of node reference, I should connect the mainfile inside
        # archive_path_info
    },
    2: {
        'name': '2',
        'type': 'workflow',
        'entry_type': 'simulation',
        'path_info': {
            'upload_id': None,
            'entry_id': None,
            'mainfile_path': '<task_2_mainfile>',
            # 'archive_path': 'workflow2',
        },
        'inputs': [
            {
                'name': 'input system from 1',
                'path_info': {
                    'section_type': 'system',
                    'supersection_index': 0,
                    'section_index': -1,
                    # 'archive_path': 'run/0/system/-1',
                },
                'out_edge_nodes': [1],
            }
        ],
        'outputs': [
            {
                'name': 'output calculation from 2',
                'path_info': {
                    'section_type': 'calculation',
                    'supersection_index': 0,
                    'calculation_index': -1,
                    # 'archive_path': 'run/0/calculation/-1'
                },
            }
        ],
    },
    3: {
        'name': '3',
        'type': 'workflow',
        'entry_type': 'simulation',
        'path_info': {
            'upload_id': None,
            'entry_id': None,
            'mainfile_path': '<task_3_mainfile>',
            # 'archive_path': 'workflow2',
        },
        'in_edge_nodes': [2],
        'out_edge_nodes': [],
    },
    4: {
        'name': 'global workflow output',
        'type': 'output',
        'entry_type': 'simulation',
        'path_info': {
            'upload_id': None,
            'entry_id': None,
            'mainfile_path': '<output_mainfile>',
            'supersection_index': -1,
            'section_type': 'results',
            # 'archive_path': 'workflow2/results/0',
        },
        'in_edge_nodes': [
            3
        ],
        # TODO if this is an output and connected node is a workflow, automatically
        # reference workflow results? Or somehow need to know to put in workflow2?
    },
}

# workflow_graph = build_nomad_workflow(
#     destination_filename='test_workflow.archive.yaml',
#     node_attributes=node_attributes,
#     write_to_yaml=True,
# )
# gv.d3(
#     workflow_graph,
#     node_label_data_source='name',
#     edge_label_data_source='name',
#     zoom_factor=1.5,
#     node_hover_tooltip=True,
# )


edges and attributes

0 1 []
0 1 []
1 6 [{'name': 'output calculation from 1', 'path_info': {'section_type': 'calculation', 'supersection_index': 0, 'calculation_index': -1, 'mainfile_path': '<task_1_mainfile>'}}]
1 6 []
1 2 []
1 2 [{'name': 'input system from 1', 'path_info': {'section_type': 'system', 'supersection_index': 0, 'section_index': -1, 'mainfile_path': '<task_1_mainfile>'}, 'out_edge_nodes': [1]}]
2 7 [{'name': 'output calculation from 2', 'path_info': {'section_type': 'calculation', 'supersection_index': 0, 'calculation_index': -1, 'mainfile_path': '<task_2_mainfile>'}}]
2 7 []
2 3 []
2 3 [{'name': 'DEFAULT input system from 2', 'path_info': {'section_type': 'system', 'mainfile_path': '<task_2_mainfile>'}}]
3 4 [{'name': 'DEFAULT output calculation from 3', 'path_info': {'section_type': 'calculation', 'mainfile_path': '<task_3_mainfile>'}}]
3 4 []
5 1 []
5 1 [{'name': 'input system from 0', 'path_info': {'section_type': 'system', 'supersection_index': 0, 'section_index': 0, 'mainfile_path': '<task_1_mainfile>'}}]



0 {'name': 'global workflow input', 'type': 'input', 'entry_type': 'simulation', 'path_info': {'upload_id': '<input_upload_id>', 'entry_id': None, 'mainfile_path': '<input_mainfile>', 'supersection_index': 0, 'section_index': 0, 'section_type': 'method'}, 'out_edge_nodes': [1]}
1 {'name': '1', 'type': 'task', 'entry_type': 'simulation', 'path_info': {'upload_id': None, 'entry_id': '<task_1_entry_id>', 'mainfile_path': '<task_1_mainfile>', 'section_type': 'workflow2'}}
2 {'name': '2', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'upload_id': None, 'entry_id': None, 'mainfile_path': '<task_2_mainfile>'}}
3 {'name': '3', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'upload_id': None, 'entry_id': None, 'mainfile_path': '<task_3_mainfile>'}, 'in_edge_nodes': [2], 'out_edge_nodes': []}
4 {'name': 'global workflow output', 'type': 'output', 'entry_type': 'simulation', 'path_info': {'upload_id': None, 'entry_id': None, 'mainfile_path': '<output_mainfile>', 'supersection_index': -1, 'section_type': 'results'}, 'in_edge_nodes': [3]}
5 {'type': 'input', 'name': 'input system from 0', 'path_info': {'section_type': 'system', 'supersection_index': 0, 'section_index': 0, 'mainfile_path': '<task_1_mainfile>'}}
6 {'type': 'output', 'name': 'output calculation from 1', 'path_info': {'section_type': 'calculation', 'supersection_index': 0, 'calculation_index': -1, 'mainfile_path': '<task_1_mainfile>'}}
7 {'type': 'output', 'name': 'output calculation from 2', 'path_info': {'section_type': 'calculation', 'supersection_index': 0, 'calculation_index': -1, 'mainfile_path': '<task_2_mainfile>'}}

