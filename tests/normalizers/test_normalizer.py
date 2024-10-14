from nomad.client import normalize_all
from nomad.datamodel import EntryArchive, EntryMetadata
from nomad.datamodel.metainfo.workflow import Workflow


def test_normalizer():
    entry_archive = EntryArchive(
        metadata=EntryMetadata(), workflow2=Workflow(name='test')
    )
    normalize_all(entry_archive)

    assert entry_archive.workflow2.name == 'test'
