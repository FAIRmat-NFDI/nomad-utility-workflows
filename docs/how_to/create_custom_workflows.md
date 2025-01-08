# How to create custom workflows with NOMAD entries

This how-to will walk you through how to use `nomad-utility-workflows` to generate the yaml file required to define a [custom workflow](https://nomad-lab.eu/prod/v1/docs/howto/customization/workflows.html){:target="_blank"}.

## Example Overview

To demonstrate, we will use the following 3 step molecular dynamics equilibration workflow:

- geometry optimization (energy minimization)

- equilibration MD simulation in NPT

- production MD simulation in NVT

The final result will be the following workflow graph visualization in NOMAD:

![NOMAD workflow graph](images/water_equilibration_workflow_graph_NOMAD.png){.screenshot}

### Example Data
Each task in this workflow represents a supported entry in NOMAD, and all 3 simulations will be uploaded together with the workflow.archive.yaml file, within the following local filesystem:

```
upload.zip
├── workflow.archive.yaml
├── Emin
│   ├── mdrun_Emin.log # Geometry Optimization mainfile
│   └── ...other raw simulation files
├── Equil_NPT
│   ├── mdrun_Equil-NPT.log # NPT equilibration mainfile
│   └── ...other raw simulation files
└── Prod_NVT
    ├── mdrun_Prod-NVT.log # NVT production mainfile
    └── ...other raw simulation files
```

You can obtain the simulation data in the GitHub repository of `nomad-utility-workflows` under [tests/utils/workflow_yaml_examples/water_equilibration/](https://github.com/FAIRmat-NFDI/nomad-utility-workflows/tree/develop/tests/utils/workflow_yaml_examples/water_equilibration){:target="_blank"} in the file `simulation_data.zip`

### Imports

First, import the necessary imports (gravis is only used for graph visualization and is not strictly necessary):

```python
import gravis as gv
import networkx as nx
from nomad_utility_workflows.utils.workflows import build_nomad_workflow, nodes_to_graph
```

## Generate the input workflow graph

To generate the appropriate workflow yaml file to connect the entries in NOMAD, we need to create a graph representating
our workflow. For this workflow, we create a `networkx.DiGraph()`, named `workflow_graph_input` in this example, that looks like:

![workflow input graph](images/water_equilibration_workflow_input_minimal.png){.screenshot}

!!! Note "IMPORTANT"
    To ensure that all functionalities work correctly, the node keys **must** be unique integers that index the nodes. I.e., `node_keys = [0, 1, 2, 3]` for a graph with 4 nodes.

If you are using a workflow manager, you can probably extract a graph structure directly from the manager output, and then map this structure to an analogous structure with networkx, as described further below. If you do not have access to such a graph, you can [Create an input graph manually](#create-an-input-graph-manually) or, alternatively, [Create an input graph with nodes_to_graph()](#create-an-input-graph-with-nodes_to_graph).

### Create an input graph manually

Use [NetworkX Docs > DiGraph](https://networkx.org/documentation/stable/reference/classes/digraph.html) to contruct a `networkx.DiGraph()`.

A series of [NodeAttributes](../reference/workflows.html#nodeattributes) should be associated with each of the nodes in your graph.
Here we print out the required node attributes in this example for the graph visualized above:

```python
for node_key, node_attributes in workflow_graph_input.nodes(data=True):
    print(node_key, node_attributes)
```

```
0 {'name': 'input system',
    'type': 'input',
    'path_info': {
        'mainfile_path': 'Emin/mdrun_Emin.log',
        'supersection_index': 0,
        'section_index': 0,
        'section_type': 'system'
    },
}

1 {'name': 'Geometry Optimization',
    'type': 'workflow',
    'entry_type': 'simulation',
    'path_info': {
        'mainfile_path': 'Emin/mdrun_Emin.log'
    }
}

2 {'name': 'Equilibration NPT Molecular Dynamics',
    'type': 'workflow',
    'entry_type': 'simulation',
    'path_info': {
        'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'
    },
}

3 {'name': 'Production NVT Molecular Dynamics',
    'type': 'workflow',
    'entry_type': 'simulation',
    'path_info': {
        'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'
    },
}

4 {'name': 'output system',
    'type': 'output',
    'path_info': {
        'section_type': 'system',
        'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'
    },
}

5 {'name': 'output properties',
    'type': 'output',
    'path_info': {
        'section_type': 'calculation',
        'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'
    },
}
```

Descriptions for each attribute are given in [Explanation > Workflow > Node Attributes](../explanation/workflows.md#node-attributes)

The appropriate edges should be added to your graph:

```python
for edge_1, edge_2, edge_attributes in workflow_graph_input.edges(data=True):
    print(edge_1, edge_2, edge_attributes)
```

```
0 1 {}
1 2 {}
2 3 {}
3 4 {}
3 5 {}
```

There are no necessary attributes to add to the edges. `nomad-utility-workflows` will automatically add edge attributes based on the node attribute inputs.

### Create an input graph with `nodes_to_graph()`

If you are unfamiliar with networkx, `nomad-utility-workflows` can generate an input graph structure for you to use with `build_workflow_yaml()`.
For this approach, simply create a dictionary of node keys and corresponding [NodeAttributes](../reference/workflows.html#nodeattributes):

```python
node_attributes = {
0: {'name': 'input system',
    'type': 'input',
    'path_info': {
        'mainfile_path': 'Emin/mdrun_Emin.log',
        'supersection_index': 0,
        'section_index': 0,
        'section_type': 'system'
    },
    'out_edge_nodes': [1],
},

1: {'name': 'Geometry Optimization',
    'type': 'workflow',
    'entry_type': 'simulation',
    'path_info': {
        'mainfile_path': 'Emin/mdrun_Emin.log'
    }
},

2: {'name': 'Equilibration NPT Molecular Dynamics',
    'type': 'workflow',
    'entry_type': 'simulation',
    'path_info': {
        'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'
    },
    'in_edge_nodes': [1],
},

3: {'name': 'Production NVT Molecular Dynamics',
    'type': 'workflow',
    'entry_type': 'simulation',
    'path_info': {
        'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'
    },
    'in_edge_nodes': [2],
},

4: {'name': 'output system',
    'type': 'output',
    'path_info': {
        'section_type': 'system',
        'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'
    },
    'in_edge_nodes': [3],
},

5: {'name': 'output properties',
    'type': 'output',
    'path_info': {
        'section_type': 'calculation',
        'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'
    },
    'in_edge_nodes': [3],
}
}
```

Again, descriptions for each attribute are given in [Explanation > Workflow > Node Attributes](../explanation/workflows.md#node-attributes)

Notice that this dictionary exactly corresponds to the printed attributes of the nodes displayed above, with the exception of the `in_edge_nodes` and `out_edge_nodes` attributes. These attributes are used to specify the graph edges.

Now, simply run:

```python
workflow_graph_input_minimal = nodes_to_graph(node_attributes)
```

The resulting graph should be identical to the input graph visualized above, and can be therefore used in the same way to [Generate the workflow yaml file](#generate-the-workflow-yaml).

## Generate the workflow yaml

Now that we have generated the input workflow graph, we can use `nomad-utility-workflows`'s [build_nomad_workflow()](../reference/workflows.html#build_nomad_workflow) function to create the `workflow.archive.yaml` file that will connect the individual example simulations within NOMAD:

```python
workflow_metadata = {
    'destination_filename': './workflow_minimal.archive.yaml',
    'workflow_name': 'Equilibration Procedure',
}

workflow_graph_output = build_nomad_workflow(
    workflow_metadata=workflow_metadata,
    workflow_graph=nx.DiGraph(workflow_graph_input),
    write_to_yaml=True,
)
```

Here we provide the full path and name of the output yaml in `destination_filename` and the overarching workflow name that will show up on top of the workflow graph visualization in `workflow_name`. The output workflow looks like:

```python
gv.d3(
    workflow_graph_output,
    node_label_data_source='name',
    edge_label_data_source='name',
    zoom_factor=1.5,
    node_hover_tooltip=True,
)
```

![workflow output graph](images/water_equilibration_workflow_output_graph_minimal.png){.screenshot}

We see that our output graph looks signficantly different than the input. That's because `nomad-utility-workflow` is automatically adding some default input/outputs to ensure the proper node connections within the workflow visualizer. For nodes with `entry_type = 'simulation'`, the automatically generated input defaults correspond to the [System](https://nomad-lab.eu/prod/v1/gui/analyze/metainfo/runschema/section_definitions@runschema.system.System){:target="_blank"} section from any incoming task node that exists. The automatically generated output defaults correspond to both the system and the [Calculation](https://nomad-lab.eu/prod/v1/gui/analyze/metainfo/runschema/section_definitions@runschema.calculation.Calculation){:target="_blank"} section from the given node.

Let's examine the output workflow graph in more detail:

```python
for node_key, node_attributes in workflow_graph_output_minimal.nodes(data=True):
    print(node_key, node_attributes)
```

```
0 {'name': 'input system', 'type': 'input', 'path_info': {'mainfile_path': 'Emin/mdrun_Emin.log', 'supersection_index': 0, 'section_index': 0, 'section_type': 'system'}}
1 {'name': 'Geometry Optimization', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Emin/mdrun_Emin.log'}}
2 {'name': 'Equilibration NPT Molecular Dynamics', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
3 {'name': 'Production NVT Molecular Dynamics', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}
4 {'name': 'output system', 'type': 'output', 'path_info': {'section_type': 'system', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}
5 {'name': 'output properties', 'type': 'output', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}
6 {'type': 'output', 'name': 'output system from Geometry Optimization', 'path_info': {'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}
7 {'type': 'output', 'name': 'output calculation from Geometry Optimization', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Emin/mdrun_Emin.log'}}
8 {'type': 'input', 'name': 'input system from Geometry Optimization', 'path_info': {'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}
9 {'type': 'output', 'name': 'output system from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
10 {'type': 'output', 'name': 'output calculation from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
11 {'type': 'input', 'name': 'input system from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
12 {'type': 'output', 'name': 'output system from Production NVT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}
13 {'type': 'output', 'name': 'output calculation from Production NVT Molecular Dynamics', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}
```

and the edges:

```python
for edge_1, edge_2, edge_attributes in workflow_graph_output_minimal.edges(data=True):
    print(edge_1, edge_2, edge_attributes)
```

```
0 1 {'inputs': [], 'outputs': []}
1 2 {'inputs': [{'name': 'output system from Geometry Optimization', 'path_info': {'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}, {'name': 'output calculation from Geometry Optimization', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Emin/mdrun_Emin.log'}}], 'outputs': [{'name': 'input system from Geometry Optimization', 'path_info': {'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}]}
1 6 {}
1 7 {}
2 3 {'inputs': [{'name': 'output system from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}, {'name': 'output calculation from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}], 'outputs': [{'name': 'input system from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}]}
2 9 {}
2 10 {}
3 4 {'inputs': [{'name': 'output system from Production NVT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}, {'name': 'output calculation from Production NVT Molecular Dynamics', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}], 'outputs': []}
3 5 {'inputs': [], 'outputs': []}
3 12 {}
3 13 {}
8 2 {}
11 3 {}
```

Finally, the output `workflow.archive.yaml`:

```yaml
'workflow2':
  'name': 'Equilibration Procedure'
  'inputs':
  - 'name': 'input system'
    'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/system/0'
  'outputs':
  - 'name': 'output system'
    'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/system/-1'
  - 'name': 'output properties'
    'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/calculation/-1'
  'tasks':
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'Geometry Optimization'
    'task': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/workflow2'
    'inputs': []
    'outputs':
    - 'name': 'output system from Geometry Optimization'
      'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/system/-1'
    - 'name': 'output calculation from Geometry Optimization'
      'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/calculation/-1'
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'Equilibration NPT Molecular Dynamics'
    'task': '../upload/archive/mainfile/Equil_NPT/mdrun_Equil-NPT.log#/workflow2'
    'inputs':
    - 'name': 'input system from Geometry Optimization'
      'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/system/-1'
    'outputs':
    - 'name': 'output system from Equilibration NPT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Equil_NPT/mdrun_Equil-NPT.log#/run/0/system/-1'
    - 'name': 'output calculation from Equilibration NPT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Equil_NPT/mdrun_Equil-NPT.log#/run/0/calculation/-1'
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'Production NVT Molecular Dynamics'
    'task': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/workflow2'
    'inputs':
    - 'name': 'input system from Equilibration NPT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Equil_NPT/mdrun_Equil-NPT.log#/run/0/system/-1'
    'outputs':
    - 'name': 'output system from Production NVT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/system/-1'
    - 'name': 'output calculation from Production NVT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/calculation/-1'
```

By uploading `workflow.archive.yaml` along with the [Example Data](#example-data), with the file structure as specified in [Example Data](#example-data), you should reproduce the workflow visualization graph seen in [Example Overview](#example-overview) within the workflow entry of your upload.


## Adding additional inputs/outputs

Now let's add some additional input/outputs to the nodes. We will use the `nodes_to_graph()` method to generate the input workflow graph:

```python
node_attributes = {
0: {'name': 'input system',
    'type': 'input',
    'path_info': {
        'mainfile_path': 'Emin/mdrun_Emin.log',
        'supersection_index': 0,
        'section_index': 0,
        'section_type': 'system'
    },
    'out_edge_nodes': [1],
},

1: {'name': 'Geometry Optimization',
    'type': 'workflow',
    'entry_type': 'simulation',
    'path_info': {
        'mainfile_path': 'Emin/mdrun_Emin.log'
    },
    'outputs': [
        {
            'name': 'energies of the relaxed system',
            'path_info': {
                'section_type': 'energy',
                'supersection_path': 'run/0/calculation', # this can be done, but at this point it's safer / easier to just use archive_path
                'supersection_index': -1,
            },
        }
    ],
},

2: {'name': 'Equilibration NPT Molecular Dynamics',
    'type': 'workflow',
    'entry_type': 'simulation',
    'path_info': {
        'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'
    },
    'in_edge_nodes': [1],
    'outputs': [
        {
            'name': 'MD workflow properties (structural and dynamical)',
            'path_info': {
                'section_type': 'results',
            },
        }
    ],
},

3: {'name': 'Production NVT Molecular Dynamics',
    'type': 'workflow',
    'entry_type': 'simulation',
    'path_info': {
        'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'
    },
    'in_edge_nodes': [2],
    'outputs': [
        {
            'name': 'MD workflow properties (structural and dynamical)',
            'path_info': {
                'section_type': 'results',
            },
        }
    ],
},

4: {'name': 'output system',
    'type': 'output',
    'path_info': {
        'section_type': 'system',
        'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'
    },
    'in_edge_nodes': [3],
},

5: {'name': 'output properties',
    'type': 'output',
    'path_info': {
        'section_type': 'calculation',
        'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'
    },
    'in_edge_nodes': [3],
}
}
```

```python
workflow_graph_input = nodes_to_graph(node_attributes)

gv.d3(
    workflow_graph_input,
    node_label_data_source='name',
    edge_label_data_source='name',
    zoom_factor=1.5,
    node_hover_tooltip=True,
)
```

![workflow output graph](images/water_equilibration_workflow_graph_input.png){.screenshot}


```python
for node_key, node_attributes in workflow_graph_input.nodes(data=True):
    print(node_key, node_attributes)
```

```
0 {'name': 'input system', 'type': 'input', 'path_info': {'mainfile_path': 'Emin/mdrun_Emin.log', 'supersection_index': 0, 'section_index': 0, 'section_type': 'system'}, 'out_edge_nodes': [1]}
1 {'name': 'Geometry Optimization', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Emin/mdrun_Emin.log'}}
2 {'name': 'Equilibration NPT Molecular Dynamics', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}, 'in_edge_nodes': [1]}
3 {'name': 'Production NVT Molecular Dynamics', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}, 'in_edge_nodes': [2]}
4 {'name': 'output system', 'type': 'output', 'path_info': {'section_type': 'system', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}, 'in_edge_nodes': [3]}
5 {'name': 'output properties', 'type': 'output', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}, 'in_edge_nodes': [3]}
6 {'type': 'output', 'name': 'energies of the relaxed system', 'path_info': {'section_type': 'energy', 'supersection_path': 'run/0/calculation', 'supersection_index': -1}}
7 {'type': 'output', 'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results'}}
8 {'type': 'output', 'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results'}}
```

```python
for edge_1, edge_2, edge_attributes in workflow_graph_input.edges(data=True):
    print(edge_1, edge_2, edge_attributes)
```

```
0 1 {}
1 6 {'inputs': [{'name': 'energies of the relaxed system', 'path_info': {'section_type': 'energy', 'supersection_path': 'run/0/calculation', 'supersection_index': -1}}]}
1 2 {}
2 7 {'inputs': [{'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results'}}]}
2 3 {}
3 8 {'inputs': [{'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results'}}]}
3 4 {}
3 5 {}
```

And again we generate the final workflow graph and yaml:

```python
workflow_graph_output = build_nomad_workflow(
    destination_filename='./workflow.archive.yaml',
    workflow_name='Equilibration Procedure',
    workflow_graph=nx.DiGraph(workflow_graph_input),
    write_to_yaml=True,
)

gv.d3(
    workflow_graph_output,
    node_label_data_source='name',
    edge_label_data_source='name',
    zoom_factor=1.5,
    node_hover_tooltip=True,
)
```

![workflow output graph](images/water_equilibration_workflow_graph_output.png){.screenshot}

```python
for node_key, node_attributes in workflow_graph_output.nodes(data=True):
    print(node_key, node_attributes)
```

```
0 {'name': 'input system', 'type': 'input', 'path_info': {'mainfile_path': 'Emin/mdrun_Emin.log', 'supersection_index': 0, 'section_index': 0, 'section_type': 'system'}, 'out_edge_nodes': [1]}
1 {'name': 'Geometry Optimization', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Emin/mdrun_Emin.log'}}
2 {'name': 'Equilibration NPT Molecular Dynamics', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}, 'in_edge_nodes': [1]}
3 {'name': 'Production NVT Molecular Dynamics', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}, 'in_edge_nodes': [2]}
4 {'name': 'output system', 'type': 'output', 'path_info': {'section_type': 'system', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}, 'in_edge_nodes': [3]}
5 {'name': 'output properties', 'type': 'output', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}, 'in_edge_nodes': [3]}
6 {'type': 'output', 'name': 'energies of the relaxed system', 'path_info': {'section_type': 'energy', 'supersection_path': 'run/0/calculation', 'supersection_index': -1, 'mainfile_path': 'Emin/mdrun_Emin.log'}}
7 {'type': 'output', 'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
8 {'type': 'output', 'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}
9 {'type': 'output', 'name': 'output system from Geometry Optimization', 'path_info': {'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}
10 {'type': 'output', 'name': 'output calculation from Geometry Optimization', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Emin/mdrun_Emin.log'}}
11 {'type': 'input', 'name': 'input system from Geometry Optimization', 'path_info': {'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}
12 {'type': 'output', 'name': 'output system from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
13 {'type': 'output', 'name': 'output calculation from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
14 {'type': 'input', 'name': 'input system from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
15 {'type': 'output', 'name': 'output system from Production NVT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}
16 {'type': 'output', 'name': 'output calculation from Production NVT Molecular Dynamics', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}
```

```python
for edge_1, edge_2, edge_attributes in workflow_graph_output.edges(data=True):
    print(edge_1, edge_2, edge_attributes)
```

```
0 1 {'inputs': [], 'outputs': []}
1 6 {'inputs': [{'name': 'energies of the relaxed system', 'path_info': {'section_type': 'energy', 'supersection_path': 'run/0/calculation', 'supersection_index': -1, 'mainfile_path': 'Emin/mdrun_Emin.log'}}, {'name': 'output system from Geometry Optimization', 'path_info': {'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}, {'name': 'output calculation from Geometry Optimization', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Emin/mdrun_Emin.log'}}], 'outputs': []}
1 2 {'inputs': [], 'outputs': [{'name': 'input system from Geometry Optimization', 'path_info': {'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}]}
1 9 {}
1 10 {}
2 7 {'inputs': [{'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}, {'name': 'output system from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}, {'name': 'output calculation from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}], 'outputs': []}
2 3 {'inputs': [], 'outputs': [{'name': 'input system from Equilibration NPT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}]}
2 12 {}
2 13 {}
3 8 {'inputs': [{'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}, {'name': 'output system from Production NVT Molecular Dynamics', 'path_info': {'section_type': 'system', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}, {'name': 'output calculation from Production NVT Molecular Dynamics', 'path_info': {'section_type': 'calculation', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}], 'outputs': []}
3 4 {'inputs': [], 'outputs': []}
3 5 {'inputs': [], 'outputs': []}
3 15 {}
3 16 {}
11 2 {}
14 3 {}
```

`workflow_archive.yaml`:
```yaml
'workflow2':
  'name': 'Equilibration Procedure'
  'inputs':
  - 'name': 'input system'
    'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/system/0'
  'outputs':
  - 'name': 'MD workflow properties (structural and dynamical)'
    'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/workflow2/results/-1'
  - 'name': 'output system'
    'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/system/-1'
  - 'name': 'output properties'
    'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/calculation/-1'
  'tasks':
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'Geometry Optimization'
    'task': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/workflow2'
    'inputs': []
    'outputs':
    - 'name': 'energies of the relaxed system'
      'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/calculation/-1/energy/-1'
    - 'name': 'output system from Geometry Optimization'
      'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/system/-1'
    - 'name': 'output calculation from Geometry Optimization'
      'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/calculation/-1'
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'Equilibration NPT Molecular Dynamics'
    'task': '../upload/archive/mainfile/Equil_NPT/mdrun_Equil-NPT.log#/workflow2'
    'inputs':
    - 'name': 'input system from Geometry Optimization'
      'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/system/-1'
    'outputs':
    - 'name': 'MD workflow properties (structural and dynamical)'
      'section': '../upload/archive/mainfile/Equil_NPT/mdrun_Equil-NPT.log#/workflow2/results/-1'
    - 'name': 'output system from Equilibration NPT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Equil_NPT/mdrun_Equil-NPT.log#/run/0/system/-1'
    - 'name': 'output calculation from Equilibration NPT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Equil_NPT/mdrun_Equil-NPT.log#/run/0/calculation/-1'
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'Production NVT Molecular Dynamics'
    'task': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/workflow2'
    'inputs':
    - 'name': 'input system from Equilibration NPT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Equil_NPT/mdrun_Equil-NPT.log#/run/0/system/-1'
    'outputs':
    - 'name': 'MD workflow properties (structural and dynamical)'
      'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/workflow2/results/-1'
    - 'name': 'output system from Production NVT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/system/-1'
    - 'name': 'output calculation from Production NVT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/calculation/-1'
```