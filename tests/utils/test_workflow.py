import numpy as np
# EXAMPLE 1 - MD Equilibration Workflow
# workflow2:
#   inputs:
#     - name: input system
#       section: '../upload/archive/mainfile/MOL0_160_MOL1_240/minimEQ1.log#/run/0/system/0'

#   outputs:
#     - name: final system of production trajectory
#       section: '../upload/archive/mainfile/MOL0_160_MOL1_240/PD1.log#/run/0/system/-1'
#     - name: final calculation of production trajectory
#       section: '../upload/archive/mainfile/MOL0_160_MOL1_240/PD1.log#/run/0/calculation/-1'

#   tasks:
#     - m_def: nomad.datamodel.metainfo.workflow.TaskReference
#       task: '../upload/archive/mainfile/MOL0_160_MOL1_240/minimEQ1.log#/workflow2'
#       name: GeometryOpt
#       inputs:
#         - name: input system
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/minimEQ1.log#/run/0/system/0'
#       outputs:
#         - name: relaxed system
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/minimEQ1.log#/run/0/system/-1'
#         - name: energy and pressure of the relaxed system
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/minimEQ1.log#/run/0/calculation/-1'
#     - m_def: nomad.datamodel.metainfo.workflow.TaskReference
#       task: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ1.log#/workflow2'
#       name: MolecularDynamics
#       inputs:
#         - name: input system
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/minimEQ1.log#/run/0/system/-1'
#       outputs:
#         - name: final system from  high temp NVT equilibration
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ1.log#/run/0/system/-1'
#         - name: final thermodynamic quantities of  high temp NVT equilibration
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ1.log#/run/0/calculation/-1'
#     - m_def: nomad.datamodel.metainfo.workflow.TaskReference
#       task: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ2.log#/workflow2'
#       name: MolecularDynamics
#       inputs:
#         - name: input system
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ1.log#/run/0/system/-1'
#       outputs:
#         - name: final system from  NVT cool down to 300k
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ2.log#/run/0/system/-1'
#         - name: final thermodynamic quantities of  NVT cool down to 300k
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ2.log#/run/0/calculation/-1'
#     - m_def: nomad.datamodel.metainfo.workflow.TaskReference
#       task: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ3.log#/workflow2'
#       name: MolecularDynamics
#       inputs:
#         - name: input system
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ2.log#/run/0/system/-1'
#       outputs:
#         - name: final system from  NPT equilibration
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ3.log#/run/0/system/-1'
#         - name: final thermodynamic quantities of  NPT equilibration
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ3.log#/run/0/calculation/-1'
#     - m_def: nomad.datamodel.metainfo.workflow.TaskReference
#       task: '../upload/archive/mainfile/MOL0_160_MOL1_240/PD1.log#/workflow2'
#       name: MolecularDynamics
#       inputs:
#         - name: input system
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/EQ3.log#/run/0/system/-1'
#       outputs:
#         - name: final system from NPT production run
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/PD1.log#/run/0/system/-1'
#         - name: final thermodynamic quantities of NPT production run
#           section: '../upload/archive/mainfile/MOL0_160_MOL1_240/PD1.log#/run/0/calculation/-1'
n_nodes = 5
adjaceny_matrix = np.zeros((n_nodes, n_nodes))
edges = [(i, i + 1) for i in range(n_nodes - 1)]
for edge in edges:
    adjaceny_matrix[edge[0], edge[1]] = 1

print(adjaceny_matrix)
node_attributes = {
    0: {'label': 'GeometryOpt',
        'type': 'GeometryOpt',
        'upload_id': None,
        'entry_id': None,
        'mainfile': None,
        'archive_path': 'workflow2',
        'non_edge_inputs': [{'name': 'input system',
                    'type': 'system',
                    'archive_path': 'run/0/system/0'}],
        'non_edge_outputs': [{'name': 'relaxed system'}]
        },
}
edge_attributes = {
    (0, 1): {'label': 'MolecularDynamics',
            'type': 'MolecularDynamics',
            'm_def': ??,
            'edge_inputs': [{'name': 'input system',
                            'type': 'system',
                            'archive_path': 'run/0/system/0'}],
            'edge_outputs': [{'name': 'relaxed system'}]
            },
    (1, 2): {'label': 'MolecularDynamics',
            'type': 'MolecularDynamics',
            'm_def': ??,
            'edge_inputs': [{'name': 'input system',
                            'type': 'system',
                            'archive_path': 'run/0/system/0'}],
            'edge_outputs': [{'name': 'relaxed system'}]
            }
}

# Add node attributes to the graph
for node, attrs in node_attributes.items():
    G.nodes[node].update(attrs)

# Add edge attributes to the graph
for edge, attrs in edge_attributes.items():
    if G.has_edge(*edge):
        G.edges[edge].update(attrs)

# Check if a specific edge exists
edge_to_check = (0, 1)
if G.has_edge(*edge_to_check):
    print(f"Edge {edge_to_check} exists.")
else:
    print(f"Edge {edge_to_check} does not exist.")
