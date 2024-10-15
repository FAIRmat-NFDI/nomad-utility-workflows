import logging
from collections import OrderedDict
from typing import Any, Literal, Optional, TypedDict, Union

import networkx as nx
import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)  # ! this is not functional I think
TASK_M_DEF = 'nomad.datamodel.metainfo.workflow.TaskReference'
WORKFLOW_M_DEF = 'nomad.datamodel.metainfo.workflow.Workflow'

SectionType = Literal['task', 'workflow', 'input', 'output']


# Define a custom representer for OrderedDict
def represent_ordereddict(dumper, data):
    return dumper.represent_dict(data.items())


# Register the custom representer
yaml.add_representer(OrderedDict, represent_ordereddict)


class PathInfo(TypedDict, total=False):
    upload_id: str
    entry_id: str
    mainfile_path: str
    supersection_path: str
    supersection_index: int
    section_type: str
    section_index: int
    archive_path: str


default_path_info = {
    'upload_id': None,
    'entry_id': None,
    'mainfile_path': '',
    'supersection_path': '',
    'supersection_index': None,
    'section_type': None,
    'section_index': -1,
    'archive_path': '',
}


class NomadSection(BaseModel):
    name: Optional[str] = Field(None, description='Name of the section')
    type: Optional[SectionType] = Field(None, description='Type of the section')
    path_info: dict[str, Any] = Field(
        default=default_path_info.copy(), description='Archive path'
    )
    inputs: list[dict[str, Any]] = Field(
        [{}],
        description='section inputs',
    )
    outputs: list[dict[str, Any]] = Field([{}], description='section outputs')

    def __init__(self, **data):
        super().__init__(**data)
        self.path_info = {**default_path_info, **self.path_info}

    @property
    def archive_path(self) -> str:
        archive_path = ''
        if not self.path_info:
            logger.warning(
                'No path info provided for %s-%s. Section reference will be missing.',
                self.type,
                self.name,
            )
            return archive_path

        if self.path_info.get('archive_path'):
            archive_path = self.path_info['archive_path']
        elif self.type == 'workflow':
            archive_path = 'workflow2'
        else:
            # SUPERSECTION
            if self.path_info[
                'supersection_path'
            ]:  # case 1 - supersection path is given
                archive_path = self.path_info['supersection_path']
                if self.path_info.get('supersection_index') is not None:
                    # add supersection index when given, else supersection is assumed
                    # to be nonrepeating
                    archive_path += f'/{self.path_info.get("supersection_index")}'
            elif self.path_info.get('section_type') in [
                'system',
                'calculation',
                'method',
            ]:  # case 2 - no supersection path, but section type is contained in run
                run_index = self.path_info.get('supersection_index')
                run_index = run_index if run_index is not None else -1
                # add run index when given, else use last run section
                archive_path = f'run/{run_index}'
            elif self.path_info.get('section_type') in ['results']:
                archive_path = 'workflow2'
            else:
                logger.warning(
                    (
                        'No supersection path provided for %s-%s. '
                        'Section reference may be incorrect.'
                    ),
                    self.type,
                    self.name,
                )

            # SECTION
            if self.path_info.get('section_type') is not None:
                archive_path += f'/{self.path_info["section_type"]}'
                if self.path_info.get('section_index') is not None:
                    archive_path += f'/{self.path_info["section_index"]}'
            else:
                logger.warning(
                    'No section type provided for %s-%s. Section reference may be incorrect.',
                    self.type,
                    self.name,
                )

        return archive_path

    @property
    def upload_prefix(self) -> str:
        if not self.path_info['mainfile_path']:
            logger.warning(
                'No mainfile path provided for %s-%s. Section reference will be missing.',
                self.type,
                self.name,
            )
            return ''

        if self.path_info.get('entry_id'):
            upload_prefix = f"/entries/{self.path_info.get('entry_id')}"
        elif self.path_info.get('upload_id'):
            upload_prefix = f"/uploads/{self.path_info.get('upload_id')}"
        else:
            upload_prefix = '../upload'

        return f"{upload_prefix}/archive/mainfile/{self.path_info['mainfile_path']}"

    @property
    def full_path(self) -> str:
        if not self.upload_prefix or not self.archive_path:
            return ''

        return f'{self.upload_prefix}#/{self.archive_path}'

    def to_dict(self) -> dict:
        return OrderedDict({'name': self.name, 'section': self.full_path})


class NomadTask(BaseModel):
    name: str
    m_def: str
    inputs: list[NomadSection] = Field(default_factory=list)
    outputs: list[NomadSection] = Field(default_factory=list)
    task_section: Optional[NomadSection] = None

    # class Config:
    #     arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        for i, input_ in enumerate(self.inputs):
            if input_.name is None:
                input_.name = f'input_{i}'
        for o, output_ in enumerate(self.outputs):
            if output_.name is None:
                output_.name = f'output_{o}'

    @property
    def m_def(self) -> str:
        if self.task_section.type == 'workflow':
            return WORKFLOW_M_DEF
        elif self.task_section.type == 'task':
            return TASK_M_DEF

    @property
    def task(self) -> Optional[str]:
        if self.task_section.type == 'workflow' and self.task_section.upload_prefix:
            return self.task_section.upload_prefix + '#/workflow2'
        else:
            return None

    def to_dict(self) -> dict:
        output_dict = OrderedDict()
        if self.m_def:
            output_dict['m_def'] = self.m_def
        output_dict['name'] = self.name
        if self.task:
            output_dict['task'] = self.task
        output_dict['inputs'] = [i.to_dict() for i in self.inputs]
        output_dict['outputs'] = [o.to_dict() for o in self.outputs]

        return output_dict


class NomadWorkflowArchive(BaseModel):
    name: str = 'workflow2'
    inputs: list[NomadSection] = Field(default_factory=list)
    outputs: list[NomadSection] = Field(default_factory=list)
    tasks: list[NomadTask] = Field(default_factory=list)

    # class Config:
    #     arbitrary_types_allowed = True

    def to_dict(self) -> dict:
        return {
            self.name: OrderedDict(
                {
                    'inputs': [i.to_dict() for i in self.inputs],
                    'outputs': [o.to_dict() for o in self.outputs],
                    'tasks': [t.to_dict() for t in self.tasks],
                }
            ),
        }

    def to_yaml(self, destination_filename: str) -> None:
        with open(destination_filename, 'w') as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False)


class NomadWorkflow(BaseModel):
    destination_filename: str
    node_attributes: dict[int, Any] = {}
    workflow_graph: nx.DiGraph = None
    task_elements: dict[str, NomadSection] = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        self.task_elements = {}
        if self.workflow_graph is None:
            self.workflow_graph = self.nodes_to_graph()
        self.fill_workflow_graph()

    def register_section(
        self, node_key: Union[int, str, tuple], node_attrs: dict[str, Any]
    ) -> None:
        section = NomadSection(**node_attrs)
        self.task_elements[node_key] = section  # ! build the tasks section by section

    # TODO Extend the graph to add nodes for the additional default inouts etc
    def nodes_to_graph(self) -> nx.DiGraph:
        if not self.node_attributes:
            logger.error(
                'No workflow graph or node attributes provided. Cannot build workflow.'
            )
            return None

        workflow_graph = nx.DiGraph()
        workflow_graph.add_nodes_from(self.node_attributes.keys())
        nx.set_node_attributes(workflow_graph, self.node_attributes)

        for node_key, node_attrs in list(workflow_graph.nodes(data=True)):
            # Add any given edges
            for edge in node_attrs.get('in_edge_nodes', []):
                workflow_graph.add_edge(edge, node_key)
            for edge in node_attrs.get('out_edge_nodes', []):
                workflow_graph.add_edge(node_key, edge)
            # Global inputs/outputs
            if node_attrs.get('type', '') == 'input':
                for edge_node in node_attrs.get('out_edge_nodes', []):
                    workflow_graph.add_edge(node_key, edge_node)
            elif node_attrs.get('type', '') == 'output':
                for edge_node in node_attrs.get('in_edge_nodes', []):
                    workflow_graph.add_edge(edge_node, node_key)

            # Task inputs/outputs
            inputs = node_attrs.pop('inputs', [])
            for input_ in inputs:
                edge_nodes = input_.get('out_edge_nodes', [])
                if not edge_nodes:
                    edge_nodes.append(len(workflow_graph.nodes))
                    workflow_graph.add_node(edge_nodes[0], type='input', **input_)

                # transfer node inputs to edge ouputs
                for edge_node in edge_nodes:
                    workflow_graph.add_edge(edge_node, node_key)
                    if not workflow_graph.edges[edge_node, node_key].get('outputs', []):
                        nx.set_edge_attributes(
                            workflow_graph, {(edge_node, node_key): {'outputs': []}}
                        )
                    workflow_graph.edges[edge_node, node_key]['outputs'].append(input_)

            outputs = node_attrs.pop('outputs', [])
            for output_ in outputs:
                edge_nodes = output_.get('in_edge_node', [])
                if not edge_nodes:
                    edge_nodes.append(len(workflow_graph.nodes))
                    workflow_graph.add_node(edge_nodes[0], type='output', **output_)

                # transfer node outputs to edge inputs
                for edge_node in edge_nodes:
                    workflow_graph.add_edge(node_key, edge_node)
                    if not workflow_graph.edges[node_key, edge_node].get('inputs', []):
                        nx.set_edge_attributes(
                            workflow_graph, {(node_key, edge_node): {'inputs': []}}
                        )
                    workflow_graph.edges[node_key, edge_node]['inputs'].append(output_)

        return workflow_graph

    # TODO Change the archive building function to loop over nodes and then add the corrsponding inputs/outputs from the edges
    def fill_workflow_graph(self) -> None:
        def get_mainfile_path(node):
            return (
                self.workflow_graph.nodes[node]
                .get('path_info', '')
                .get('mainfile_path', '')
            )

        def check_for_defaults(inout_type, default_section, edge) -> bool:
            inout_type = 'inputs' if inout_type == 'outputs' else 'outputs'
            for input_ in edge.get(inout_type, []):
                if (
                    input_.get('path_info', {}).get('section_type', '')
                    == default_section
                ):
                    return True
            return False

        def get_defaults(
            inout_type: Literal['inputs', 'outputs'], node_source, node_dest
        ) -> list:
            defaults = {
                'inputs': {
                    'section': 'system',
                },
                'outputs': {
                    'section': 'calculation',
                },
            }
            partner_node = node_source
            node_source_type = self.workflow_graph.nodes[node_source].get('type', '')
            if node_source_type == 'input':
                partner_node = node_dest

            default_section = defaults[inout_type]['section']
            flag_defaults = False
            if inout_type == 'outputs':
                for _, _, edge2 in self.workflow_graph.out_edges(
                    node_source, data=True
                ):
                    if check_for_defaults(inout_type, default_section, edge2):
                        flag_defaults = True
                        break
            elif inout_type == 'inputs':
                for _, _, edge2 in self.workflow_graph.in_edges(node_dest, data=True):
                    if check_for_defaults(inout_type, default_section, edge2):
                        flag_defaults = True
                        break
            if flag_defaults:
                return []

            partner_name = self.workflow_graph.nodes[partner_node].get('name', '')
            inouts = [
                {
                    'name': f'DEFAULT {inout_type[:-1]} {default_section} from {partner_name}',
                    'path_info': {
                        'section_type': default_section,
                        'mainfile_path': get_mainfile_path(partner_node),
                    },
                },
            ]

            return inouts

        # resolve mainfile for all edge inouts and add defaults
        for node_source, node_dest, edge in self.workflow_graph.edges(data=True):
            # EDGE INPUTS
            if not edge.get('inputs'):
                nx.set_edge_attributes(
                    self.workflow_graph, {(node_source, node_dest): {'inputs': []}}
                )
            for input_ in edge['inputs']:
                if not input_.get('path_info', {}):
                    continue
                if not input_['path_info'].get('mainfile_path', ''):
                    # edge inputs always coming from the source node
                    input_['path_info']['mainfile_path'] = get_mainfile_path(
                        node_source
                    )

            # EDGE OUTPUTS
            if not edge.get('outputs'):
                nx.set_edge_attributes(
                    self.workflow_graph, {(node_source, node_dest): {'outputs': []}}
                )
            for output_ in edge.get('outputs', []):
                if not output_.get('path_info', {}):
                    continue
                if not output_['path_info'].get('mainfile_path', ''):
                    node_source_type = self.workflow_graph.nodes[node_source].get(
                        'type', ''
                    )
                    # edge output assigned to source unless source is an input node
                    if (
                        node_source_type == 'input'
                    ):  # ! assuming here that the input is coming from the same archive, but will not be assigned anyway if path_info is empty for this node
                        output_['path_info']['mainfile_path'] = get_mainfile_path(
                            node_dest
                        )
                    else:
                        output_['path_info']['mainfile_path'] = get_mainfile_path(
                            node_source
                        )

            # ADD DEFAULTS
            # ? Here I am added the default to the first edge in case they are missing, not positive this covers all cases correctly
            # edge_input is source output
            if self.workflow_graph.nodes[node_source].get('type', '') in [
                'task',
                'workflow',
            ]:
                for outputs_ in get_defaults('outputs', node_source, node_dest):
                    edge['inputs'].append(outputs_)

            # edge_output is dest input
            if self.workflow_graph.nodes[node_dest].get('type', '') in [
                'task',
                'workflow',
            ]:
                for inputs_ in get_defaults('inputs', node_source, node_dest):
                    edge['outputs'].append(inputs_)

    def build_workflow_yaml(self) -> None:
        # register the sections and build task_elements
        # register the nodes as sections for the archive construction
        for (
            node_key,
            node_attrs,
        ) in self.workflow_graph.nodes(data=True):
            self.register_section(node_key, node_attrs)

        archive = self.generate_archive()
        archive.to_yaml(self.destination_filename)

    def generate_archive(self) -> NomadWorkflowArchive:
        archive = NomadWorkflowArchive()
        archive.inputs = []
        archive.outputs = []

        for node_key, node in self.workflow_graph.nodes(data=True):
            if node.get('type', '') == 'input':
                element = self.task_elements[node_key]
                archive.inputs.append(element)
            elif node.get('type', '') == 'output':
                element = self.task_elements[node_key]
                archive.outputs.append(element)
            elif node.get('type', '') in ['task', 'workflow']:
                inputs = []
                outputs = []
                for _, _, edge in self.workflow_graph.out_edges(node_key, data=True):
                    if edge.get('inputs'):
                        outputs.extend(edge.get('inputs'))
                for _, _, edge in self.workflow_graph.in_edges(node_key, data=True):
                    if edge.get('outputs'):
                        inputs.extend(edge.get('outputs'))

                archive.tasks.append(
                    NomadTask(
                        name=node.get('name', ''),
                        inputs=inputs,
                        outputs=outputs,
                        task_section=self.task_elements[node_key],
                    )
                )
        return archive


def build_nomad_workflow(
    destination_filename: str = './nomad_workflow.archive.yaml',
    node_attributes: dict[int, Any] = {},
    workflow_graph: nx.DiGraph = None,
    write_to_yaml: bool = False,
) -> nx.DiGraph:
    workflow = NomadWorkflow(
        destination_filename=destination_filename,
        node_attributes=node_attributes,
        workflow_graph=workflow_graph,
    )
    if write_to_yaml:
        workflow.build_workflow_yaml()

    return workflow.workflow_graph


# TODO make sure that the post_nomad etc with authentication are passing the correct urls without sections added!
# TODO add is_simulation, is_nomad_entry as flags
# TODO test this code on a number of already existing examples
# TODO create docs with some examples for dict and graph input types
# TODO add to readme/docs that this is not currently using NOMAD, but could be linked later?
# TODO should nodes_to_graph() be an external function from the class? So, that the user can call it, but also add attributes from there?
# TODO add some text to the test notebooks

# TODO change the rest of the functions to pydantic -- not sure if I really want to tackle this now
