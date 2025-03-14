import nomad_utility_workflows.utils.workflows as nuw


def test_main_path():
    ns = nuw.NomadSection(
        path_info={
            'mainfile_path': 'vibrational_analysis.archive.yaml',
            'entry_id': 'Qq0VGiABIgx9HQtpYL4RLdONuUp1',
            'archive_path': 'data'
        },        
    )
    assert ns.full_path == '/entries/Qq0VGiABIgx9HQtpYL4RLdONuUp1/archive#/data'

    ns.path_info['upload_id'] = 'J3lTsIQBRxuGBJQFd6uaKA'
    assert ns.full_path == '/uploads/J3lTsIQBRxuGBJQFd6uaKA/archive/Qq0VGiABIgx9HQtpYL4RLdONuUp1#/data'
