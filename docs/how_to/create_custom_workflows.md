# How to create custom workflows with NOMAD entries
<!-- The implementation of this how-to is in nomad-utility-workflows/tests/utils/workflow_yaml_examples/water_equilibration -->

This how-to will walk you through how to use `nomad-utility-workflows` to generate the yaml file required to define a [custom workflow](https://nomad-lab.eu/prod/v1/docs/howto/customization/workflows.html){:target="_blank"}.

## Prerequisites

* A very basic understanding of NOMAD terminology: [entry](https://nomad-lab.eu/prod/v1/docs/reference/glossary.html#entry){:target="_blank"}, [mainfile](https://nomad-lab.eu/prod/v1/docs/reference/glossary.html#mainfile){:target="_blank"}
* [Python environment with this utility module](./install_this_plugin.md)
* Example simulation data files (provided below)


## Example Overview

To demonstrate, we will use the following 3 step molecular dynamics equilibration workflow:

1. Geometry optimization (energy minimization)
2. Equilibration MD simulation in NPT ensemble
3. Production MD simulation in NVT ensemble

The final result will be a workflow graph visualization in NOMAD that looks like this:

<div class="click-zoom">
    <label>
        <input type="checkbox">
        <img src="images/water_equilibration_workflow_graph_NOMAD.png" alt="" width="100%" title="Click to zoom in">
    </label>
</div>

<!--- TODO: have a link to a comparable self-contained nomad upload. -->



### Example Data Structure

Each task in this workflow represents a supported entry in NOMAD. All three simulations will be uploaded together with the `workflow.archive.yaml` file, within the following structure:

```
upload.zip
├── workflow.archive.yaml  # The file we'll create in this guide
├── Emin
│   ├── mdrun_Emin.log     # Geometry Optimization mainfile
│   └── ...other raw simulation files
├── Equil_NPT
│   ├── mdrun_Equil-NPT.log  # NPT equilibration mainfile
│   └── ...other raw simulation files
└── Prod_NVT
    ├── mdrun_Prod-NVT.log   # NVT production mainfile
    └── ...other raw simulation files
```

[Download Example Data](../assets/simulation_data.zip){:target="_blank" .md-button}


## Complete Workflow Creation Example

Let's create the workflow from start to finish. First, import the necessary packages:

```python
import gravis as gv

from nomad_utility_workflows.utils.workflows import (
    NodeAttributes,
    NodeAttributesUniverse,
    NomadWorkflow,
    nodes_to_graph,
)
```

### Step 1: Define the workflow structure

<!-- TODO I think simplify this example to the bare minimum and then add some further smaller extension examples at the end -->

We'll define our workflow structure using a dictionary of [NodeAttributes](../reference/workflows.md#nodeattributes):

```python
node_attributes = {
    0: NodeAttributes(
        name='input system',
        type='input',
        path_info={
            'mainfile_path': 'Emin/mdrun_Emin.log',
            'section_type': 'system',
        },
        out_edge_nodes=[1],
    ),
    1: NodeAttributes(
        name='Geometry Optimization',
        type='workflow',
        entry_type='simulation',
        path_info={'mainfile_path': 'Emin/mdrun_Emin.log'},
    ),
    2: NodeAttributes(
        name='Equilibration NPT Molecular Dynamics',
        type='workflow',
        entry_type='simulation',
        path_info={'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'},
        in_edge_nodes=[1],
    ),
    3: NodeAttributes(
        name='Production NVT Molecular Dynamics',
        type='workflow',
        entry_type='simulation',
        path_info={'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'},
        in_edge_nodes=[2],
    ),
    4: NodeAttributes(
        name='output system',
        type='output',
        path_info={
            'section_type': 'system',
            'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log',
        },
        in_edge_nodes=[3],
    ),
    5: NodeAttributes(
        name='output properties',
        type='output',
        path_info={
            'section_type': 'calculation',
            'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log',
        },
        in_edge_nodes=[3],
    ),
}

node_attributes_universe = NodeAttributesUniverse(nodes=node_attributes)
```

!!! Note "IMPORTANT"

    To ensure that all functionalities work correctly, the node keys **must** be unique integers that index the nodes. For example, `node_keys = [0, 1, 2, 3, 4, 5]` for a graph with 6 nodes.

### Step 2: Create the workflow graph

Now, convert the node attributes dictionary to a graph using [`nodes_to_graph()`](../reference/workflows.md#nodes_to_graph) and display the resulting workflow graph with gravis (gv):

```python
workflow_graph_input_minimal = nodes_to_graph(node_attributes_universe)

gv.d3(
    workflow_graph_input_minimal,
    node_label_data_source='name',
    edge_label_data_source='name',
    zoom_factor=1.5,
    node_hover_tooltip=True,
)
```

The visualization of the input graph should look like this:

<div class="click-zoom">
    <label>
        <input type="checkbox">
        <img src="images/water_equilibration_workflow_input_minimal.png" alt="" width="100%" title="Click to zoom in">
    </label>
</div>


### Step 3: Fill the workflow graph with default connections

Before creating the workflow YAML, the utility module will fill the workflow graph generated in the previous step with certain "default" connections (i.e., inputs and outputs) based on the types of nodes in the workflow. This happens automatically upon instantiation of the [`NomadWorkflow`](../reference/workflows.md#nomadworkflow) class:

```python
nomad_workflow = NomadWorkflow(
    workflow_graph=workflow_graph_input_minimal,
)
```

Here, we create a `NomadWorkflow` using the workflow graph generated in the previous step. The resulting "filled" workflow graph can be visualized with:

```javascript
gv.d3(
    nomad_workflow.workflow_graph,
    node_label_data_source='name',
    edge_label_data_source='name',
    zoom_factor=1.5,
    node_hover_tooltip=True,
)
```

<div class="click-zoom">
    <label>
        <input type="checkbox">
        <img src="images/water_equilibration_workflow_output_graph_minimal.png" alt="" width="100%" title="Click to zoom in">
    </label>
</div>

In this case, because the nodes have `entry_type = 'simulation'`, the automatically generated outputs include:

* The [System](https://nomad-lab.eu/prod/v1/gui/analyze/metainfo/runschema/section_definitions@runschema.system.System){:target="_blank"} section
* The [Calculation](https://nomad-lab.eu/prod/v1/gui/analyze/metainfo/runschema/section_definitions@runschema.calculation.Calculation){:target="_blank"} section


### Step 4: Generate the workflow YAML

Finally, we can add some workflow metadata to the [`NomadWorkflow`](../reference/workflows.md#nomadworkflow) (i.e., the filename of the output yaml and the name of the workflow) and generate the workflow YAML file with the class method [`build_workflow_yaml()`](../reference/workflows.md#build_workflow_yaml):

```python
nomad_workflow.destination_filename = './workflow_minimal.archive.yaml'
nomad_workflow.name = 'Equilibration Procedure'
nomad_workflow.build_workflow_yaml()
```

The resulting `workflow_minimal.archive.yaml` file will look like this:

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
    'inputs':
    - 'name': 'input run/0/system/0 from input system'
      'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/system/0'
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
    - 'name': 'output run/0/system/-1 from output system'
      'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/system/-1'
    - 'name': 'output system from Production NVT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/system/-1'
    - 'name': 'output calculation from Production NVT Molecular Dynamics'
      'section': '../upload/archive/mainfile/Prod_NVT/mdrun_Prod-NVT.log#/run/0/calculation/-1'
```

## Uploading and Viewing Your Workflow

After generating the `workflow.archive.yaml` file:


1. Place it in the root directory of your example data, matching the [Example Data Structure](#example-data-structure) above
2. Upload to NOMAD via the [API](./use_api_functions.md) or [Drag-and-Drop](https://nomad-lab.eu/prod/v1/docs/howto/manage/upload.html)
3. Navigate to the entry overview page associated with the `workflow.archive.yaml` file.

You should see a workflow visualization identical to the one in [Example Overview](#example-overview).

<!-- TODO Maybe update this with links to the tutorials when they are added -->


## Alternative Approach: Creating a Graph Manually

If you prefer to create the graph structure directly with NetworkX instead of using `nodes_to_graph()`, you can do so:

```python
# Create an empty directed graph
workflow_graph_input = nx.DiGraph()

# Add nodes with attributes
workflow_graph_input.add_node(0,
    name='input system',
    type='input',
    path_info={
        'mainfile_path': 'Emin/mdrun_Emin.log',
        'supersection_index': 0,
        'section_index': 0,
        'section_type': 'system'
    }
)

workflow_graph_input.add_node(1,
    name='Geometry Optimization',
    type='workflow',
    entry_type='simulation',
    path_info={
        'mainfile_path': 'Emin/mdrun_Emin.log'
    }
)

# Add more nodes...

# Add edges to connect the nodes
workflow_graph_input.add_edge(0, 1)
workflow_graph_input.add_edge(1, 2)
# Add more edges...
```

## Adding additional inputs/outputs

You can add inputs/outputs beyond the defaults set by the utility by simply adding them to the node attributes dictionary:

```python
node_attributes = {
    0: NodeAttributes(
        name='input system',
        type='input',
        path_info={
            'mainfile_path': 'Emin/mdrun_Emin.log',
            'section_type': 'system',
        },
        out_edge_nodes=[1],
    ),
    1: NodeAttributes(
        name='Geometry Optimization',
        type='workflow',
        entry_type='simulation',
        path_info={'mainfile_path': 'Emin/mdrun_Emin.log'},
        outputs=[
            {
                'name': 'energies of the relaxed system',
                'path_info': {
                    'section_type': 'energy',
                    'supersection_path': 'run/0/calculation',  # this can be done,
                    # or use archive_path='/run/0/calculation/-1/energy',
                    'supersection_index': -1,
                },
            }
        ],
    ),
    2: NodeAttributes(
        name='Equilibration NPT Molecular Dynamics',
        type='workflow',
        entry_type='simulation',
        path_info={'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'},
        in_edge_nodes=[1],
        outputs=[
            {
                'name': 'MD workflow properties (structural and dynamical)',
                'path_info': {
                    'section_type': 'results',
                },
            }
        ],
    ),
    3: NodeAttributes(
        name='Production NVT Molecular Dynamics',
        type='workflow',
        entry_type='simulation',
        path_info={'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'},
        in_edge_nodes=[2],
        outputs=[
            {
                'name': 'MD workflow properties (structural and dynamical)',
                'path_info': {
                    'section_type': 'results',
                },
            }
        ],
    ),
    4: NodeAttributes(
        name='output system',
        type='output',
        path_info={
            'section_type': 'system',
            'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log',
        },
        in_edge_nodes=[3],
    ),
    5: NodeAttributes(
        name='output properties',
        type='output',
        path_info={
            'section_type': 'calculation',
            'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log',
        },
        in_edge_nodes=[3],
    ),
}

node_attributes_universe = NodeAttributesUniverse(nodes=node_attributes)
```

In general, unless you want to view the unfilled workflow graph, we can create, fill, and write the workflow all in one step:

```python
nomad_workflow = NomadWorkflow(
    node_attributes_universe=node_attributes_universe,
    destination_filename='./workflow.archive.yaml',
    name='Equilibration Procedure',
)
nomad_workflow.build_workflow_yaml()
```

The final graph can be view as before:

```python
gv.d3(
    nomad_workflow.workflow_graph,
    node_label_data_source='name',
    edge_label_data_source='name',
    zoom_factor=1.5,
    node_hover_tooltip=True,
)
```

<div class="click-zoom">
    <label>
        <input type="checkbox">
        <img src="images/water_equilibration_workflow_graph_output.png" alt="" width="100%" title="Click to zoom in">
    </label>
</div>

To inspect the graph in detail, you can explicitly print the nodes and edges along with their attributes:

```python
for node_key, node_attributes in workflow_graph_output.nodes(data=True):
    print(node_key, node_attributes)
```

??? Success "output"
    ```
    0 {'name': 'input system', 'type': 'input', 'entry_type': None, 'path_info': {'mainfile_path': 'Emin/mdrun_Emin.log', 'section_type': 'system', 'archive_path': 'run/0/system/-1'}, 'in_edge_nodes': [], 'out_edge_nodes': [1]}
    1 {'name': 'Geometry Optimization', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Emin/mdrun_Emin.log', 'archive_path': 'workflow2'}, 'in_edge_nodes': [], 'out_edge_nodes': []}
    2 {'name': 'Equilibration NPT Molecular Dynamics', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log', 'archive_path': 'workflow2'}, 'in_edge_nodes': [1], 'out_edge_nodes': []}
    3 {'name': 'Production NVT Molecular Dynamics', 'type': 'workflow', 'entry_type': 'simulation', 'path_info': {'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log', 'archive_path': 'workflow2'}, 'in_edge_nodes': [2], 'out_edge_nodes': []}
    4 {'name': 'output system', 'type': 'output', 'entry_type': None, 'path_info': {'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log', 'section_type': 'system', 'archive_path': 'run/0/system/-1'}, 'in_edge_nodes': [3], 'out_edge_nodes': []}
    5 {'name': 'output properties', 'type': 'output', 'entry_type': None, 'path_info': {'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log', 'section_type': 'calculation', 'archive_path': 'run/0/calculation/-1'}, 'in_edge_nodes': [3], 'out_edge_nodes': []}
    6 {'type': 'output', 'name': 'energies of the relaxed system', 'path_info': {'section_type': 'energy', 'supersection_path': 'run/0/calculation', 'supersection_index': -1, 'mainfile_path': 'Emin/mdrun_Emin.log', 'archive_path': 'run/0/calculation/-1/energy/-1'}}
    7 {'type': 'output', 'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log', 'archive_path': 'workflow2/results/-1'}}
    8 {'type': 'output', 'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log', 'archive_path': 'workflow2/results/-1'}}
    9 {'type': 'input', 'name': 'input run/0/system/-1 from input system', 'path_info': {'mainfile_path': 'Emin/mdrun_Emin.log', 'section_type': 'system', 'archive_path': 'run/0/system/-1'}, 'is_default': True}
    10 {'type': 'output', 'name': 'output system from Geometry Optimization', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}
    11 {'type': 'output', 'name': 'output calculation from Geometry Optimization', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'calculation', 'mainfile_path': 'Emin/mdrun_Emin.log'}}
    12 {'type': 'input', 'name': 'input system from Geometry Optimization', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}
    13 {'type': 'output', 'name': 'output system from Equilibration NPT Molecular Dynamics', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
    14 {'type': 'output', 'name': 'output calculation from Equilibration NPT Molecular Dynamics', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'calculation', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
    15 {'type': 'input', 'name': 'input system from Equilibration NPT Molecular Dynamics', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}
    16 {'type': 'output', 'name': 'output system from Production NVT Molecular Dynamics', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'system', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}
    17 {'type': 'output', 'name': 'output calculation from Production NVT Molecular Dynamics', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'calculation', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}
    ```

```python
for edge_1, edge_2, edge_attributes in workflow_graph_output.edges(data=True):
    print(edge_1, edge_2, edge_attributes)
```

??? Success "output"
    ```
    0 1 {'inputs': [], 'outputs': [{'name': 'input run/0/system/-1 from input system', 'path_info': {'mainfile_path': 'Emin/mdrun_Emin.log', 'section_type': 'system', 'archive_path': 'run/0/system/-1'}, 'is_default': True}]}
    1 6 {'inputs': [{'name': 'energies of the relaxed system', 'path_info': {'section_type': 'energy', 'supersection_path': 'run/0/calculation', 'supersection_index': -1, 'mainfile_path': 'Emin/mdrun_Emin.log', 'archive_path': 'run/0/calculation/-1/energy/-1'}}, {'name': 'output system from Geometry Optimization', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}, {'name': 'output calculation from Geometry Optimization', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'calculation', 'mainfile_path': 'Emin/mdrun_Emin.log'}}], 'outputs': []}
    1 2 {'inputs': [], 'outputs': [{'name': 'input system from Geometry Optimization', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'system', 'mainfile_path': 'Emin/mdrun_Emin.log'}}]}
    1 10 {}
    1 11 {}
    2 7 {'inputs': [{'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log', 'archive_path': 'workflow2/results/-1'}}, {'name': 'output system from Equilibration NPT Molecular Dynamics', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}, {'name': 'output calculation from Equilibration NPT Molecular Dynamics', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'calculation', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}], 'outputs': []}
    2 3 {'inputs': [], 'outputs': [{'name': 'input system from Equilibration NPT Molecular Dynamics', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'system', 'mainfile_path': 'Equil_NPT/mdrun_Equil-NPT.log'}}]}
    2 13 {}
    2 14 {}
    3 8 {'inputs': [{'name': 'MD workflow properties (structural and dynamical)', 'path_info': {'section_type': 'results', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log', 'archive_path': 'workflow2/results/-1'}}, {'name': 'output system from Production NVT Molecular Dynamics', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'system', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}, {'name': 'output calculation from Production NVT Molecular Dynamics', 'path_info': {'entry_id': None, 'upload_id': None, 'section_type': 'calculation', 'mainfile_path': 'Prod_NVT/mdrun_Prod-NVT.log'}}], 'outputs': []}
    3 4 {'inputs': [], 'outputs': []}
    3 5 {'inputs': [], 'outputs': []}
    3 16 {}
    3 17 {}
    9 1 {}
    12 2 {}
    15 3 {}
    ```

The resulting `workflow.archive.yaml` file will look like:

```yaml
'workflow2':
  'name': 'Equilibration Procedure'
  'inputs':
  - 'name': 'input system'
    'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/system/-1'
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
    'inputs':
    - 'name': 'input run/0/system/-1 from input system'
      'section': '../upload/archive/mainfile/Emin/mdrun_Emin.log#/run/0/system/-1'
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

And after uploading to NOMAD with the simulation files as before, you should get a workflow entry with the visualization graph:

<div class="click-zoom">
    <label>
        <input type="checkbox">
        <img src="images/workflow_graph_NOMAD_add_outputs.png" alt="" width="100%" title="Click to zoom in">
    </label>
</div>

By clicking on the middle task box "Equilibration NPT Mole...", you will open the sub-workflow where the additional inputs and outputs added to this task are clearly visible:

<div class="click-zoom">
    <label>
        <input type="checkbox">
        <img src="images/workflow_graph_NOMAD_subtask_add_inouts.png" alt="" width="100%" title="Click to zoom in">
    </label>
</div>

## References

For more details on node attributes and other options, see:

* [Reference > Workflows](../reference/workflows.md)


