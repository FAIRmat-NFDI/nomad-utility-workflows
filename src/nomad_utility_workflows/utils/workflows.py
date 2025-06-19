import logging
from collections import OrderedDict
from typing import Any, Literal, Optional, Union

import networkx as nx
import yaml
from pydantic import BaseModel, Field, computed_field
from typing_extensions import TypedDict

logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format='%(levelname)s: %(message)s',
)

logger = logging.getLogger(__name__)
TASK_M_DEF = 'nomad.datamodel.metainfo.workflow.TaskReference'
WORKFLOW_M_DEF = 'nomad.datamodel.metainfo.workflow.TaskReference'
# TODO not yet sure about the specification of actual tasks, need to test

SectionType = Literal['task', 'workflow', 'input', 'output']
EntryType = Literal['simulation']


# Define a custom representer for OrderedDict
def represent_ordereddict(dumper, data):
    return dumper.represent_dict(data.items())


class SingleQuotedScalarString(str):
    pass


def single_quoted_scalar_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'")


yaml.add_representer(SingleQuotedScalarString, single_quoted_scalar_representer)
yaml.add_representer(str, single_quoted_scalar_representer)

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
    entry_type: Optional[EntryType] = Field(None, description='Type of the entry')
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
        if not self.path_info:
            logger.warning(
                f'No path info provided for {self.type}-{self.name}.'
                ' Section reference will be missing.'
            )
            return ''

        if self.path_info.get('archive_path'):
            return self.path_info['archive_path']
        elif self.type == 'workflow' or self.entry_type == 'simulation':
            return 'workflow2'
        elif self.type == 'task':
            return 'data'
        else:
            return self._get_supersection_path()

    def _get_supersection_path(self) -> str:
        archive_path = ''
        if self.path_info.get('supersection_path'):
            archive_path = self._get_supersection_path_with_index()
        elif self.path_info.get('section_type'):
            archive_path = self._get_supersection_path_from_section_type()
        else:
            logger.warning(
                (
                    'No supersection path or section type provided for '
                    f'{self.type}-{self.name}. Section reference may be incorrect.'
                ),
            )
        return self._get_section_path(archive_path)

    def _get_supersection_path_with_index(self) -> str:
        archive_path = self.path_info['supersection_path']
        if self.path_info.get('supersection_index') is not None:
            archive_path += f"/{self.path_info.get('supersection_index')}"
        return archive_path

    def _get_supersection_path_from_section_type(self) -> str:
        archive_path = ''
        if self.path_info.get('section_type') in ['system', 'calculation', 'method']:
            # get default run supersection for simulation sections
            run_index = self.path_info.get('supersection_index', 0)
            run_index = run_index if run_index is not None else 0
            archive_path = f'run/{run_index}'
        elif self.path_info.get('section_type') in ['results']:
            archive_path = 'workflow2'
        # ? Is this required in some case?
        # ! It appears to be a duplicate of get_section_path
        # else:
        #     archive_path += f"/{self.path_info.get('section_type')}"
        #     if self.path_info.get('section_index') is not None:
        #         archive_path += f"/{self.path_info.get('section_index')}"
        return archive_path

    def _get_section_path(self, archive_path: str) -> str:
        if self.path_info.get('section_type') is not None:
            archive_path += f"/{self.path_info['section_type']}"
            if self.path_info.get('section_index') is not None:
                archive_path += f"/{self.path_info['section_index']}"
        else:
            # TODO: this may be is a false warning if the subsection_path is given
            logger.warning(
                (
                    f'No section type provided for {self.type}-{self.name}. '
                    'Section reference may be incorrect.'
                ),
            )
        return archive_path

    @property
    def upload_prefix(self) -> str:
        if not self.path_info['mainfile_path']:
            logger.warning(
                f'No mainfile path provided for {self.type}-{self.name}. '
                'Section reference will be missing.'
            )
            return ''

        if self.path_info.get('entry_id') and self.path_info.get('upload_id'):
            upload_prefix = (
                f"../uploads/{self.path_info.get('upload_id')}/archive/"
                f"{self.path_info.get('entry_id')}"
            )
        elif self.path_info.get('entry_id'):
            # TODO - remove when entry_id only references supported
            logger.warning(
                'upload_id missing. entry_id only references not yet supported.'
                'reference will be missing.'
            )
            upload_prefix = f"../entries/{self.path_info.get('entry_id')}/archive"
        elif self.path_info.get('upload_id'):
            # TODO - remove when upload_id only references supported
            logger.warning(
                'entry_id missing. upload_id only references not yet supported.'
                'reference will be missing.'
            )
            upload_prefix = (
                f"../uploads/{self.path_info.get('upload_id')}/archive/mainfile/"
                f"{self.path_info['mainfile_path']}"
            )
        else:
            upload_prefix = (
                f"../upload/archive/mainfile/{self.path_info['mainfile_path']}"
            )

        return upload_prefix

    @property
    def full_path(self) -> str:
        if not self.upload_prefix or not self.archive_path:
            return ''

        return f"{self.upload_prefix}#/{self.archive_path}{''}"

    def to_dict(self) -> dict:
        return OrderedDict(
            {'name': self.name, 'section': SingleQuotedScalarString(self.full_path)}
        )


class NomadTask(BaseModel):
    name: str
    inputs: list[NomadSection] = Field(default_factory=list)
    outputs: list[NomadSection] = Field(default_factory=list)
    task_section: Optional[NomadSection] = None

    def __init__(self, **data):
        super().__init__(**data)
        for i, input_ in enumerate(self.inputs):
            if input_.name is None:
                input_.name = f'input_{i}'
        for o, output_ in enumerate(self.outputs):
            if output_.name is None:
                output_.name = f'output_{o}'

    @computed_field
    @property
    def m_def(self) -> str:
        if self.task_section.type == 'workflow':
            return WORKFLOW_M_DEF
        elif self.task_section.type == 'task':
            return TASK_M_DEF

    @computed_field
    @property
    def task(self) -> Optional[str]:
        if self.task_section.type == 'workflow' and self.task_section.upload_prefix:
            return self.task_section.upload_prefix + '#/workflow2'
        elif self.task_section.type == 'task' and self.task_section.full_path:
            return self.task_section.full_path
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
    archive_section: str = None
    m_def: Optional[str] = None
    name: Optional[str] = None
    inputs: list[NomadSection] = Field(default_factory=list)
    outputs: list[NomadSection] = Field(default_factory=list)
    tasks: list[NomadTask] = Field(default_factory=list)

    def remove_duplicate_ios(self) -> None:
        def remove_duplicates(ios):
            seen = set()
            trimmed = []
            for io in ios:
                if io.full_path not in seen:
                    trimmed.append(io)
                    seen.add(io.full_path)
            return trimmed

        self.inputs = remove_duplicates(self.inputs)
        self.outputs = remove_duplicates(self.outputs)

    def to_dict(self) -> dict:
        yaml_dict = {self.archive_section: OrderedDict({})}
        if self.m_def:
            yaml_dict[self.archive_section]['m_def'] = self.m_def
        if self.name:
            yaml_dict[self.archive_section]['name'] = self.name
        if self.inputs:
            yaml_dict[self.archive_section]['inputs'] = [
                i.to_dict() for i in self.inputs
            ]
        if self.outputs:
            yaml_dict[self.archive_section]['outputs'] = [
                o.to_dict() for o in self.outputs
            ]
        if self.tasks:
            yaml_dict[self.archive_section]['tasks'] = [t.to_dict() for t in self.tasks]

        return yaml_dict

    def to_yaml(self, destination_filename: str) -> None:
        with open(destination_filename, 'w') as f:
            yaml.dump(
                self.to_dict(),
                f,
                default_flow_style=False,
                allow_unicode=True,
                width=80,
            )


class NodeAttributes(BaseModel):
    """
    NodeAttributes represents the attributes of a node in the NOMAD workflow graph.

    Attributes:
        name (str): A free-form string describing this node, which will be used as a
                    label in the NOMAD workflow graph visualizer.

        type (Literal['input', 'output', 'workflow', 'task']):
            Specifies the type of node. Must be one of the specified options.

            - input: (meta)data taken as input for the entire workflow or a specific
                    task. For simulations, often corresponds to a section within the
                    archive (e.g., system, method).

            - output: (meta)data produced as output for the entire workflow or a
                    specific task. For simulations, often corresponds to a section
                    within the archive (e.g., calculation).

            - workflow: A node in the workflow which itself contains an internal
                    (sub)workflow, that is recognized by NOMAD. Such nodes can be
                    linked to existing workflows within NOMAD, providing
                    functionalities within NOMAD's interactive workflow graphs.

            - task: A node in the workflow which represents an individual task
                    (i.e., no underlying workflow), that is recognized by NOMAD.

        entry_type (Literal['simulation']): Specifies the type of node in terms of
            tasks or workflows recognized by NOMAD. Functionally, this attribute is
            used to create default inputs and outputs that are required for properly
            creating the edge visualizations in the NOMAD GUI.

        path_info (dict): Information for generating the NOMAD archive section paths
            (i.e., connections between nodes in terms of the NOMAD MetaInfo sections).

            - upload_id (str): NOMAD PID for the upload, if exists.

            - entry_id (str): NOMAD PID for the entry, if exists.

            - mainfile_path (str): Local (relative to the native upload) path to the
                mainfile, including the mainfile name with extension.

            - supersection_path (str): Archive path to the supersection, e.g.,
                "run" or "workflow2/method".

            - supersection_index (int): The relevant index for the supersection, if it
                is a repeating subsection.

            - section_type (str): The name of the section for an input or output node,
                e.g., "system", "method", or "calculation".

            - section_index (int): The relevant index for the section, if it is a
                repeating subsection.

            - archive_path (str): Specifies the entire archive path to the section,
                e.g., "run/0/system/2".

        inputs (list[dict]): A list of input nodes to be added to the graph with
            in_edges to the parent node.

            - name (str): Will be set as the name for the input node created.

            - path_info (dict): Path information for the input node created, as
                specified for the node attributes above.

        outputs (list[dict]): A list of output nodes to be added to the graph with
            out_edges from the parent node.

            - name (str): Will be set as the name for the output node created.

            - path_info (dict): Path information for the output node created, as
                specified for the node attributes above.

        in_edge_nodes (list[int]): A list of integers specifying the node keys which
            contain in-edges to this node.

        out_edge_nodes (list[int]): A list of integers specifying the node keys which
            contain out-edges to this node.
    """

    name: str = Field(None, description='A free-form string describing this node.')
    type: SectionType = Field(None, description='The type of node.')
    entry_type: EntryType = Field(
        None, description='The type of node recognized by NOMAD.'
    )
    path_info: PathInfo = Field(
        None, description='Information for generating the NOMAD archive section paths.'
    )
    inputs: list[dict[str, Any]] = Field(
        default_factory=list,
        description='A list of input nodes to be added to the graph.',
    )
    outputs: list[dict[str, Any]] = Field(
        default_factory=list,
        description='A list of output nodes to be added to the graph.',
    )
    in_edge_nodes: list[int] = Field(
        default_factory=list, description='Nodes with in-edges to this node.'
    )
    out_edge_nodes: list[int] = Field(
        default_factory=list, description='Nodes with out-edges to this node.'
    )

    def __init__(self, **data):
        super().__init__(**data)
        if self.type == 'workflow':
            if not self.path_info:
                return
            if not self.path_info.get('archive_path'):
                self.path_info['archive_path'] = 'workflow2'

    def get(self, key: str, default: Any = None) -> Any:
        """
        Allows dictionary-like access to the attributes of the NodeAttributes class.

        Args:
            key (str): The attribute name to retrieve.
            default (Any): The default value to return if the attribute is not found.

        Returns:
            Any: The value of the attribute if it exists, otherwise the default value.
        """
        return getattr(self, key, default)


class NodeAttributesUniverse(BaseModel):
    """
    Container for all node attributes in a NOMAD workflow graph.

    This class holds a mapping from node keys (integers) to their corresponding
    `NodeAttributes` objects, representing the full set of nodes in a workflow.
    It is used as the input for building the workflow graph and for serializing
    or deserializing workflow definitions.

    Attributes:
        nodes (dict[int, NodeAttributes]):
            Dictionary mapping node keys to their attributes.
    """

    nodes: dict[int, NodeAttributes]


class NomadWorkflow(BaseModel):
    """
    Represents a NOMAD workflow and provides methods to build and serialize it.

    This class manages the workflow graph, node attributes, and the logic for
    constructing a NOMAD-compatible workflow archive. It supports registering
    nodes, resolving edges, adding default sections, and exporting the workflow
    to a YAML file for use with NOMAD systems.

    Attributes:
        destination_filename (str):
            Path to the output YAML file for the workflow archive.
        m_def (str):
            NOMAD m_def path for the workflow type.
        name (str):
            User-defined name for the workflow.
        archive_section (str):
            Root section of the archive to store the workflow.
        node_attributes_universe (NodeAttributesUniverse):
            Universe of node attributes for the workflow graph.
        workflow_graph (nx.DiGraph):
            Directed graph representing the workflow structure.
        task_elements (dict[str, NomadSection]):
            Registered sections for each node in the workflow.
        simulation_default_sections (dict[str, list[str]]):
            Default input and output sections for simulation tasks.
    """

    destination_filename: str = Field(
        './nomad_custom_workflow_archive.yaml',
        description='The full path and filename to write the output yaml file.',
    )
    m_def: str = Field(
        None, description='The NOMAD m_def path for a specific workflow type.'
    )
    name: str = Field(None, description='User-defined name for the workflow.')
    archive_section: str = Field(
        'workflow2',
        description='The root section of the archive to store the workflow.',
    )
    node_attributes_universe: NodeAttributesUniverse = Field(default_factory=dict)
    workflow_graph: nx.DiGraph = None
    task_elements: dict[str, NomadSection] = Field(default_factory=dict)
    simulation_default_sections: dict[str, list[str]] = Field(
        default_factory=dict,
        description='Default input and output sections for simulation tasks',
    )

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        self.task_elements = {}
        self.simulation_default_sections = {
            'inputs': ['system'],
            'outputs': ['system', 'calculation'],
        }
        # ! add more defaults here
        if self.workflow_graph is None:
            self.workflow_graph = nodes_to_graph(self.node_attributes_universe)
        self._fill_workflow_graph()

    def register_section(
        self, node_key: Union[int, str, tuple], node_attrs: dict[str, Any]
    ) -> None:
        section = NomadSection(**node_attrs)
        self.task_elements[node_key] = section

    def _fill_workflow_graph(self) -> None:
        """_summary_"""
        for node_source, node_dest, edge in list(self.workflow_graph.edges(data=True)):
            self._resolve_edge_inputs(node_source, node_dest, edge)
            self._resolve_edge_outputs(node_source, node_dest, edge)
            self._add_defaults(node_source, node_dest, edge)

    def _resolve_edge_inputs(self, node_source, node_dest, edge) -> None:
        if not edge.get('inputs'):
            nx.set_edge_attributes(
                self.workflow_graph, {(node_source, node_dest): {'inputs': []}}
            )
        for input_ in edge['inputs']:
            if not input_.get('path_info', {}):
                continue
            if not input_['path_info'].get('mainfile_path', ''):
                input_['path_info']['mainfile_path'] = self._get_mainfile_path(
                    node_source
                )

    def _resolve_edge_outputs(self, node_source, node_dest, edge) -> None:
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
                if node_source_type == 'input':
                    output_['path_info']['mainfile_path'] = self._get_mainfile_path(
                        node_dest
                    )
                else:
                    output_['path_info']['mainfile_path'] = self._get_mainfile_path(
                        node_source
                    )

    def _add_defaults(self, node_source, node_dest, edge) -> None:
        if self.workflow_graph.nodes[node_source].get('type', '') in [
            'task',
            'workflow',
        ]:
            for outputs_ in self._get_defaults('outputs', node_source, node_dest):
                edge['inputs'].append(outputs_)
                # add the output to the graph
                self.workflow_graph.add_node(
                    len(self.workflow_graph.nodes), type='output', **outputs_
                )
                self.workflow_graph.add_edge(
                    node_source, len(self.workflow_graph.nodes) - 1
                )
        if self.workflow_graph.nodes[node_dest].get('type', '') in ['task', 'workflow']:
            for inputs_ in self._get_defaults('inputs', node_source, node_dest):
                edge['outputs'].append(inputs_)
                # add the input to the graph
                self.workflow_graph.add_node(
                    len(self.workflow_graph.nodes), type='input', **inputs_
                )
                self.workflow_graph.add_edge(
                    len(self.workflow_graph.nodes) - 1, node_dest
                )

    def _get_mainfile_path(self, node):
        return (
            self.workflow_graph.nodes[node]
            .get('path_info', '')
            .get('mainfile_path', '')
        )

    def _check_for_defaults(self, inout_type, edge, proposed_path_info) -> bool:
        # ! The current method would prevent multiple inputs from the same section
        # TODO - look into removing duplicates after the registration of the section
        inout_type = 'inputs' if inout_type == 'outputs' else 'outputs'
        for input_ in edge.get(inout_type, []):
            if input_.get('path_info', {}).get(
                'section_type', ''
            ) == proposed_path_info.get('section_type', ''):
                if input_.get('path_info', {}).get(
                    'mainfile_path', ''
                ) == proposed_path_info.get('mainfile_path', ''):
                    return True
            # ? Not sure if this second case is needed
            if input_.get('path_info', {}).get(
                'supersection_path', ''
            ) == proposed_path_info.get('section_type', ''):
                if input_.get('path_info', {}).get(
                    'mainfile_path', ''
                ) == proposed_path_info.get('mainfile_path', ''):
                    return True
        return False

    def _get_defaults(
        self, inout_type: Literal['inputs', 'outputs'], node_source, node_dest
    ) -> list:
        entry_type_source = self.workflow_graph.nodes[node_source].get('entry_type', '')
        node_source_type = self.workflow_graph.nodes[node_source].get('type', '')
        entry_type_dest = self.workflow_graph.nodes[node_dest].get('entry_type', '')
        node_dest_type = self.workflow_graph.nodes[node_dest].get('type', '')

        inouts = []
        if inout_type == 'outputs':  # Gets outputs for the source node
            if node_dest_type == 'output':
                partner_node = node_dest
                # adds the global output
                inouts += self._get_general_defaults(
                    inout_type, partner_node, node_source, node_dest
                )
            if entry_type_source == 'simulation':
                partner_node = node_source
                # adds output sys + calc
                inouts += self._get_simulation_defaults(
                    inout_type, partner_node, node_source, node_dest
                )
            if not inouts:
                partner_node = node_source
                # adds the general archive path of source to its output
                inouts += self._get_general_defaults(
                    inout_type, partner_node, node_source, node_dest
                )
        elif inout_type == 'inputs':  # Gets inputs for the dest node
            if node_source_type == 'input':
                partner_node = node_source
                # adds the global input
                inouts += self._get_general_defaults(
                    inout_type, partner_node, node_source, node_dest
                )
            elif entry_type_source == 'simulation':
                partner_node = node_source
                # adds the output sys + calc? from source to dest input
                inouts += self._get_simulation_defaults(
                    inout_type, partner_node, node_source, node_dest
                )
            else:
                partner_node = node_source
                # adds the general archive path of source to its input
                inouts += self._get_general_defaults(
                    inout_type, partner_node, node_source, node_dest
                )
                if entry_type_dest == 'simulation':
                    partner_node = node_dest
                    # adds the input sys from dest to its own input
                    inouts += self._get_simulation_defaults(
                        inout_type, partner_node, node_source, node_dest
                    )

        return inouts

    def _get_general_defaults(self, inout_type, partner_node, node_source, node_dest):
        section = NomadSection(**self.workflow_graph.nodes[partner_node])
        archive_path = section.archive_path
        default_sections = {
            'inputs': [archive_path],
            'outputs': [archive_path],
        }

        inouts = []
        for default_section in default_sections[inout_type]:
            partner_name = self.workflow_graph.nodes[partner_node].get('name', '')
            proposed_path_info = self.workflow_graph.nodes[partner_node].get(
                'path_info', {}
            )
            proposed_path_info['archive_path'] = default_section
            if not self._flag_defaults(
                inout_type, node_source, node_dest, proposed_path_info
            ):
                inouts.append(
                    {
                        'name': (
                            f'{inout_type[:-1]} {default_section} '
                            f'from {partner_name}'
                        ),
                        'path_info': proposed_path_info,
                        'is_default': True,
                    },
                )
        return inouts

    def _get_simulation_defaults(
        self, inout_type, partner_node, node_source, node_dest
    ):
        default_sections = self.simulation_default_sections
        inouts = []
        for default_section in default_sections[inout_type]:
            partner_name = self.workflow_graph.nodes[partner_node].get('name', '')
            # TODO - check this when reassessing the method for simulation defaults
            path_info = self.workflow_graph.nodes[partner_node].get('path_info', {})
            proposed_path_info = {}
            proposed_path_info['entry_id'] = path_info.get('entry_id', None)
            proposed_path_info['upload_id'] = path_info.get('upload_id', None)
            proposed_path_info['section_type'] = default_section
            proposed_path_info['mainfile_path'] = self._get_mainfile_path(partner_node)

            if not self._flag_defaults(
                inout_type,
                node_source,
                node_dest,
                proposed_path_info,
            ):
                inouts.append(
                    {
                        'name': (
                            f'{inout_type[:-1]} {default_section} '
                            f'from {partner_name}'
                        ),
                        'path_info': proposed_path_info,
                    },
                )
        return inouts

    def _flag_defaults(
        self,
        inout_type,
        node_source,
        node_dest,
        proposed_path_info,
    ):
        flag_defaults = False
        if inout_type == 'outputs':
            for _, _, edge2 in self.workflow_graph.out_edges(node_source, data=True):
                if self._check_for_defaults(inout_type, edge2, proposed_path_info):
                    flag_defaults = True
                    break
        elif inout_type == 'inputs':
            for _, _, edge2 in self.workflow_graph.in_edges(node_dest, data=True):
                if self._check_for_defaults(inout_type, edge2, proposed_path_info):
                    flag_defaults = True
                    break
        return flag_defaults

    def build_workflow_yaml(self) -> None:
        """
        Construct and serialize the workflow archive to a YAML file.

        This method registers all nodes in the workflow graph as sections, builds the
        internal task elements, generates the workflow archive, removes duplicate
        inputs/outputs, and writes the resulting archive to the YAML file specified by
        `self.destination_filename`. The resulting YAML file is compatible with the
        NOMAD workflow schema and can be used for further processing or import into
        NOMAD systems.

        After writing the file, a summary message is logged with the output filename.
        """
        # register the sections and build task_elements
        # register the nodes as sections for the archive construction
        for (
            node_key,
            node_attrs,
        ) in self.workflow_graph.nodes(data=True):
            self.register_section(node_key, node_attrs)

        archive = self.generate_archive()
        archive.remove_duplicate_ios()
        archive.to_yaml(self.destination_filename)
        logger.info(f'NOMAD workflow written to {self.destination_filename}')

    def generate_archive(self) -> NomadWorkflowArchive:
        archive = NomadWorkflowArchive(
            archive_section=self.archive_section, m_def=self.m_def, name=self.name
        )
        archive.inputs = []
        archive.outputs = []

        task_graph = self._create_task_graph()
        self._add_inputs_to_archive(archive, task_graph)
        self._add_outputs_to_archive(archive, task_graph)
        self._add_tasks_to_archive(archive, task_graph)

        return archive

    def _create_task_graph(self) -> nx.DiGraph:
        task_nodes = [
            n
            for n, attr in self.workflow_graph.nodes(data=True)
            if attr.get('type', '') in ['task', 'workflow']
        ]
        return self.workflow_graph.subgraph(task_nodes)

    def _add_inputs_to_archive(
        self, archive: NomadWorkflowArchive, task_graph: nx.DiGraph
    ) -> None:
        for node in [n for n, d in task_graph.in_degree if d == 0]:
            for edge in self.workflow_graph.in_edges(node, data=True):
                if self.workflow_graph.nodes[edge[0]].get('type', '') != 'input':
                    continue
                if self.workflow_graph.nodes[edge[0]].get('is_default', '') is True:
                    continue
                element = self.task_elements[edge[0]]
                archive.inputs.append(element)

    def _add_outputs_to_archive(
        self, archive: NomadWorkflowArchive, task_graph: nx.DiGraph
    ) -> None:
        for node in [n for n, d in task_graph.out_degree if d == 0]:
            for edge in self.workflow_graph.out_edges(node, data=True):
                if self.workflow_graph.nodes[edge[1]].get('type', '') != 'output':
                    continue
                if self.workflow_graph.nodes[edge[1]].get('is_default', '') is True:
                    continue
                element = self.task_elements[edge[1]]
                archive.outputs.append(element)

    def _add_tasks_to_archive(
        self, archive: NomadWorkflowArchive, task_graph: nx.DiGraph
    ) -> None:
        for node_key, node in task_graph.nodes(data=True):
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


def nodes_to_graph(node_attributes_universe: 'NodeAttributesUniverse') -> nx.DiGraph:
    """Builds a workflow graph (nx.DiGraph) from a NodeAttributesUniverse of node
    attributes as specified below.

    Args:
        node_attributes_universe: A NodeAttributesUniverse object containing the node

    Returns:
        nx.DiGraph: _description_
    """
    if not node_attributes_universe:
        raise TypeError(
            'No workflow graph or node attributes provided. Cannot build workflow.'
        )

    workflow_graph = nx.DiGraph()
    workflow_graph.add_nodes_from(node_attributes_universe.nodes.keys())
    nx.set_node_attributes(workflow_graph, node_attributes_universe.nodes)

    for node_key, node_attrs in list(workflow_graph.nodes(data=True)):
        _add_edges(workflow_graph, node_key, node_attrs)
        _add_global_inouts(workflow_graph, node_key, node_attrs)
        _add_task_inouts(workflow_graph, node_key, node_attrs)

    return workflow_graph


def _add_edges(workflow_graph, node_key, node_attrs):
    def set_mainfile_path(workflow_graph, edge, node_attrs) -> None:
        parent_mainfile_path = (
            workflow_graph.nodes[edge].get('path_info', '').get('mainfile_path', None)
        )
        if not node_attrs.get('path_info'):
            node_attrs['path_info'] = {'mainfile_path': parent_mainfile_path}
        else:
            node_attrs['path_info']['mainfile_path'] = node_attrs['path_info'].get(
                'mainfile_path', parent_mainfile_path
            )

    for edge in node_attrs.get('in_edge_nodes', []):
        workflow_graph.add_edge(edge, node_key)
        set_mainfile_path(workflow_graph, edge, node_attrs)
    for edge in node_attrs.get('out_edge_nodes', []):
        workflow_graph.add_edge(node_key, edge)
        set_mainfile_path(workflow_graph, edge, node_attrs)


def _add_global_inouts(workflow_graph, node_key, node_attrs):
    if node_attrs.get('type', '') == 'input':
        for edge_node in node_attrs.get('out_edge_nodes', []):
            workflow_graph.add_edge(node_key, edge_node)
    elif node_attrs.get('type', '') == 'output':
        for edge_node in node_attrs.get('in_edge_nodes', []):
            workflow_graph.add_edge(edge_node, node_key)


def _add_task_inouts(workflow_graph, node_key, node_attrs):
    inputs = node_attrs.pop('inputs', [])
    for input_ in inputs:
        edge_nodes = input_.get('out_edge_nodes', [])
        if not edge_nodes:
            edge_nodes.append(len(workflow_graph.nodes))
            workflow_graph.add_node(edge_nodes[0], type='input', **input_)

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

        for edge_node in edge_nodes:
            workflow_graph.add_edge(node_key, edge_node)
            if not workflow_graph.edges[node_key, edge_node].get('inputs', []):
                nx.set_edge_attributes(
                    workflow_graph, {(node_key, edge_node): {'inputs': []}}
                )
            workflow_graph.edges[node_key, edge_node]['inputs'].append(output_)


# TODO prevent duplicates to global outputs
# TODO the input from in edge task nodes are automatically added to the global inputs...
# TODO but not vice versa, the reverse should be done...
# TODO also prevent that the same ios are added 2x
# TODO I need to check that the defaults are generated properly when you have multiple
# input or output task nodes.
# TODO we need to fix the default inputs, so that system[-1] is not added, and instead
# either the global input or possibly system[0] only
# TODO -1 notation doesn't work for run for connections!!
# TODO test this code on a number of already existing examples
# TODO create docs with some examples for dict and graph input types
# TODO add to readme/docs that this is not currently using NOMAD, but could be linked
# later?
# TODO add some text to the test notebooks
# TODO change the rest of the functions to pydantic -- not sure if I really want to
# tackle this now
# TODO advanced defaults for simulation --
# -- look into a subworkflow for the output system from the final task
