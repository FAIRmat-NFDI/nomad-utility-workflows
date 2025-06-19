# How to add custom tasks to workflows using NOMAD's ELN Functionalities
<!-- Implemented in nomad-utility-workflows/tests/utils/workflow_yaml_examples/solute_in_bilayer/ -->

This how-to covers how to add a custom NOMAD entry in the case that some tasks or inputs/outputs of your workflow are not automatically parsed and stored within a NOMAD entry, i.e., preventing you to reference these within your workflow.


## Example Overview

Consider the following setup, simulation, and analysis protocol:

<div class="click-zoom">
    <label>
        <input type="checkbox">
        <img src="images/solute_in_bilayer_workflow_graph_NOMAD.png" alt="" width="100%" title="Click to zoom in">
    </label>
</div>

[Download Example Data](../assets/solute_in_bilayer.zip){:target="_blank" .md-button}

The minimize, equilibrate, and production (workflow) tasks are analogous to that described in [How to > Create Custom Workflows](./create_custom_workflows.md). The remaining tasks correspond to steps in the simulation protocol that are not supported by the NOMAD simulation parsers, e.g., creation of the initial configuration or model parameter files, or post-simulation analysis.

## Create an ELN entry with ElnBaseSection

The basic strategy described here is to use NOMAD's existing ELN functionality to add (meta)data for the "non-recognized" steps of the workflow. Here we only provide a description quantity, but more advanced support is available, see [NOMAD Docs > How to > Use ELNs](https://nomad-lab.eu/prod/v1/docs/howto/manage/eln.html){:target="_blank"}.

Before we can add these as tasks in the workflow, we need to create mainfiles that NOMAD recognizes with the corresponding metadata. We can achieve this by creating an archive.yaml file as follows:

```yaml
# insert_solute_in_box.archive.yaml
data:
  m_def: nomad.datamodel.metainfo.eln.ElnBaseSection
  name: 'insert_solute_in_box'
  description: 'This is a description of the method performed to insert the solute into the simulation box...'
```

This utilizes the `ELNBaseSection` class to create the following overview page upon upload:

<div class="click-zoom">
    <label>
        <input type="checkbox">
        <img src="images/ELN_overview_page.png" alt="" width="90%" title="Click to zoom in">
    </label>
</div>

Similarly, the remaining (non-simulation) custom workflow steps (i.e., tasks) can be included in a similar manner:

??? abstract "workflow_parameters.archive.yaml"
    ```yaml
    data:
        m_def: nomad.datamodel.metainfo.eln.ElnBaseSection
        name: 'workflow_parameters'
        description: 'This is a description of the overall workflow parameters, or alternatively standard workflow specification...'
    ```
??? abstract "convert_box_to_gro.archive.yaml"
    ```yaml
    data:
        m_def: nomad.datamodel.metainfo.eln.ElnBaseSection
        name: 'convert_box_to_gro'
        description: 'This is a description of the method performed to create the initial gro file...'
    ```
??? abstract "update_topology_file.archive.yaml"
    ```yaml
    data:
        m_def: nomad.datamodel.metainfo.eln.ElnBaseSection
        name: 'update_topology_file'
        description: 'This is a description of the method performed to update the topology file...'
    ```
??? abstract "compute_wham.archive.yaml"
    ```yaml
    data:
         m_def: nomad.datamodel.metainfo.eln.ElnBaseSection
        name: 'compute_wham'
        description: 'This is a description of the application of the wham method...'
    ```

All custom yaml entry files are already included in the example data provided above.

## Link the ELN entries to your workflow

Now that we have a mainfile for each task, we can specify the graph strucuture and node attributes as described in the [Create Custom Workflows > Complete Workflow Creation Example](./create_custom_workflows.md#complete-workflow-creation-example):

```python
import gravis as gv

from nomad_utility_workflows.utils.workflows import (
    NodeAttributesUniverse,
    NodeAttributes,
    NomadWorkflow,
    nodes_to_graph,
)

path_to_job = ''
node_attributes_universe = NodeAttributesUniverse(
    nodes={
        0: NodeAttributes(
            name="Solute in bilayer workflow parameters",
            type="input",
            path_info={
                'archive_path': 'data',
                'mainfile_path': f'{path_to_job}workflow_parameters.archive.yaml',
            },
            out_edge_nodes=[1, 3],
        ),

        1: NodeAttributes(
            name="insert_solute_in_box",
            type="task",
            path_info={
                'mainfile_path': f'{path_to_job}insert_solute_in_box.archive.yaml',
                'archive_path': 'data',
            },
        ),

        2: NodeAttributes(
            name="convert_box_to_gro",
            type="task",
            path_info={
                'mainfile_path': f'{path_to_job}convert_box_to_gro.archive.yaml',
                'archive_path': 'data',
            },
            in_edge_nodes=[1],
        ),

        3: NodeAttributes(
            name="update_topology_file",
            type="task",
            path_info={
                'mainfile_path': f'{path_to_job}update_topology_file.archive.yaml',
                'archive_path': 'data',
            },
        ),

        4: NodeAttributes(
            name="minimize",
            type="workflow",
            entry_type="simulation",
            in_edge_nodes=[2, 3],
            path_info={
                'mainfile_path': f'{path_to_job}solute_in_bilayer_minimize.log'
            },
        ),


        5: NodeAttributes(
            name="equilibrate",
            type="workflow",
            entry_type="simulation",
            path_info={
                'mainfile_path': f'{path_to_job}solute_in_bilayer_equilibrate.log'
            },
            in_edge_nodes=[4],
        ),

        6: NodeAttributes(
            name="production",
            type="workflow",
            entry_type="simulation",
            path_info={
                'mainfile_path': f'{path_to_job}solute_in_bilayer_production.log'
            },
            in_edge_nodes=[5],
            out_edge_nodes=[7],
        ),

        7: NodeAttributes(
            name='WHAM Analysis',
            type='output',
            path_info={
                'archive_path': 'data',
                'mainfile_path': f'{path_to_job}compute_wham.archive.yaml'
            },
        ),
    }
)
```

## Generate the input workflow graph and workflow yaml

Identically to `Create Custom Workflows >` [Complete Workflow Creation Example](./create_custom_workflows.md#complete-workflow-creation-example) and [Generate the workflow yaml](./create_custom_workflows.md#step-4-generate-the-workflow-yaml), we create a `NomadWorkflow` object with the appropriate quantities:

```python
nomad_workflow = NomadWorkflow(
    node_attributes_universe=node_attributes_universe,
    destination_filename='solute_in_bilayer.workflow.archive.yaml',
    name='Solute in bilayer workflow',
)
nomad_workflow.build_workflow_yaml()
```

which produces the following workflow yaml:

```yaml
'workflow2':
  'name': 'Solute in bilayer workflow'
  'inputs':
  - 'name': 'Solute in bilayer workflow parameters'
    'section': '../upload/archive/mainfile/workflow_parameters.archive.yaml#/data'
  'outputs':
  - 'name': 'WHAM Analysis'
    'section': '../upload/archive/mainfile/compute_wham.archive.yaml#/data'
  - 'name': 'output system from production'
    'section': '../upload/archive/mainfile/solute_in_bilayer_production.log#/run/0/system/-1'
  - 'name': 'output calculation from production'
    'section': '../upload/archive/mainfile/solute_in_bilayer_production.log#/run/0/calculation/-1'
  'tasks':
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'insert_solute_in_box'
    'task': '../upload/archive/mainfile/insert_solute_in_box.archive.yaml#/data'
    'inputs':
    - 'name': 'input data from Solute in bilayer workflow parameters'
      'section': '../upload/archive/mainfile/workflow_parameters.archive.yaml#/data'
    'outputs':
    - 'name': 'output data from insert_solute_in_box'
      'section': '../upload/archive/mainfile/insert_solute_in_box.archive.yaml#/data'
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'convert_box_to_gro'
    'task': '../upload/archive/mainfile/convert_box_to_gro.archive.yaml#/data'
    'inputs':
    - 'name': 'input data from insert_solute_in_box'
      'section': '../upload/archive/mainfile/insert_solute_in_box.archive.yaml#/data'
    'outputs':
    - 'name': 'output data from convert_box_to_gro'
      'section': '../upload/archive/mainfile/convert_box_to_gro.archive.yaml#/data'
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'update_topology_file'
    'task': '../upload/archive/mainfile/update_topology_file.archive.yaml#/data'
    'inputs':
    - 'name': 'input data from Solute in bilayer workflow parameters'
      'section': '../upload/archive/mainfile/workflow_parameters.archive.yaml#/data'
    'outputs':
    - 'name': 'output data from update_topology_file'
      'section': '../upload/archive/mainfile/update_topology_file.archive.yaml#/data'
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'minimize'
    'task': '../upload/archive/mainfile/solute_in_bilayer_minimize.log#/workflow2'
    'inputs':
    - 'name': 'input data from convert_box_to_gro'
      'section': '../upload/archive/mainfile/convert_box_to_gro.archive.yaml#/data'
    - 'name': 'input system from minimize'
      'section': '../upload/archive/mainfile/solute_in_bilayer_minimize.log#/run/0/system/-1'
    - 'name': 'input data from update_topology_file'
      'section': '../upload/archive/mainfile/update_topology_file.archive.yaml#/data'
    'outputs':
    - 'name': 'output system from minimize'
      'section': '../upload/archive/mainfile/solute_in_bilayer_minimize.log#/run/0/system/-1'
    - 'name': 'output calculation from minimize'
      'section': '../upload/archive/mainfile/solute_in_bilayer_minimize.log#/run/0/calculation/-1'
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'equilibrate'
    'task': '../upload/archive/mainfile/solute_in_bilayer_equilibrate.log#/workflow2'
    'inputs':
    - 'name': 'input system from minimize'
      'section': '../upload/archive/mainfile/solute_in_bilayer_minimize.log#/run/0/system/-1'
    'outputs':
    - 'name': 'output system from equilibrate'
      'section': '../upload/archive/mainfile/solute_in_bilayer_equilibrate.log#/run/0/system/-1'
    - 'name': 'output calculation from equilibrate'
      'section': '../upload/archive/mainfile/solute_in_bilayer_equilibrate.log#/run/0/calculation/-1'
  - 'm_def': 'nomad.datamodel.metainfo.workflow.TaskReference'
    'name': 'production'
    'task': '../upload/archive/mainfile/solute_in_bilayer_production.log#/workflow2'
    'inputs':
    - 'name': 'input system from equilibrate'
      'section': '../upload/archive/mainfile/solute_in_bilayer_equilibrate.log#/run/0/system/-1'
    'outputs':
    - 'name': 'output data from WHAM Analysis'
      'section': '../upload/archive/mainfile/compute_wham.archive.yaml#/data'
    - 'name': 'output system from production'
      'section': '../upload/archive/mainfile/solute_in_bilayer_production.log#/run/0/system/-1'
    - 'name': 'output calculation from production'
      'section': '../upload/archive/mainfile/solute_in_bilayer_production.log#/run/0/calculation/-1'
```

When uploaded to NOMAD with the corresponding simulation files and ELN `archive.yaml`'s, you should obtain a workflow entry with the visualization show the top of this page.

## References

For more details on creating and customizing ELN entries within a workflow, see:

* [Tutorial > Part 3: Creating Custom Entries in NOMAD](https://fairmat-nfdi.github.io/nomad-tutorial-workflows/latest/custom/){:target="\_blank"}