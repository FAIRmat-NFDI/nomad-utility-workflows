import nomad_utility_workflows.utils.workflows as nuw


def test_main_path():
    upload_id = '<fill_in>'
    entry_id = '<fill_in>'
    ns = nuw.NomadSection(
        path_info={
            'mainfile_path': 'vibrational_analysis.archive.yaml',
            'upload_id': upload_id,
            'entry_id': entry_id,
            'archive_path': 'data',
        },
    )
    assert ns.full_path == f'/uploads/{upload_id}/archive/{entry_id}#/data'
