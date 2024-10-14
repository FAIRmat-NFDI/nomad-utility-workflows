from nomad.config.models.plugins import ParserEntryPoint
from pydantic import Field


class NewParserEntryPoint(ParserEntryPoint):
    parameter: int = Field(0, description='Custom configuration parameter')

    def load(self):
        from nomad_utility_workflows.parsers.parser import NewParser

        return NewParser(**self.dict())


parser_entry_point = NewParserEntryPoint(
    name='NewParser',
    description='New parser entry point configuration.',
    mainfile_name_re='.*\.newmainfilename',
)
