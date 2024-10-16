# How to use nomad-utility-workflows to perform NOMAD API Calls

Imports for the following examples:


```python
import time
from pprint import pprint

from decouple import config as environ

from nomad_utility_workflows.utils.core import get_authentication_token
from nomad_utility_workflows.utils.datasets import (
    create_dataset,
    delete_dataset,
    get_dataset_by_id,
    retrieve_datasets,
)
from nomad_utility_workflows.utils.entries import (
    download_entry_by_id,
    get_entries_of_my_uploads,
    get_entries_of_upload,
    get_entry_by_id,
    query_entries,
)
from nomad_utility_workflows.utils.uploads import (
    delete_upload,
    edit_upload_metadata,
    get_all_my_uploads,
    get_upload_by_id,
    publish_upload,
    upload_files_to_nomad,
)
from nomad_utility_workflows.utils.users import (
    get_user_by_id,
    search_users_by_name,
    who_am_i,
)
```

## NOMAD URLs

The NOMAD URL specifies the base address of the API for the NOMAD deployment of interest. Typically, this URL is structured as `https://<deployment_base_path>/api/v1`.

By default, nomad-utility-workflows uses the Test deployment of NOMAD to make API calls. This is simply a safety mechanism so that users do not accidentally publish something during testing. 

All API functions allow the user to specify the URL with the optional keyword argument `url`. If you want to use the central NOMAD URLs, you can simply set `url` equal to "prod", "staging", or "test", which correspond to the following deployments (see full URLs below):

- prod: the official NOMAD deployment. 
    - Updated most infrequently (as advertised in #software-updates on the NOMAD Discord Server)
- staging: the beta version of NOMAD. 
    - Updated more frequently than prod in order to integrate and test new features. 
- test: a test NOMAD deployment. 
    - The data is occassionally wiped, such that test publishing can be made.

Note that the prod and staging deployments share a common database, and that publishing on either will result in publically available data.

Alternatively to these short names, the user can use the `url` input to specify the full API address to some alternative NOMAD deployment, e.g., an Oasis.

For reference, the full addresses of the above-mentioned central NOMAD deployments (including api suffix) are:


```python
from nomad_utility_workflows.utils.core import (
    NOMAD_PROD_URL,
    NOMAD_STAGING_URL,
    NOMAD_TEST_URL,
)

print(NOMAD_PROD_URL, NOMAD_STAGING_URL, NOMAD_TEST_URL)
```

    https://nomad-lab.eu/prod/v1/api/v1 https://nomad-lab.eu/prod/v1/staging/api/v1 https://nomad-lab.eu/prod/v1/test/api/v1


## Authentication

Some API calls, e.g., making uploads or accessing your own non-published uploads, require an authentication token. To generate this token, nomad-utility-workflows expects that your NOMAD credentials are stored in a `.env` file in the plugin root directory in the format:

```bash
NOMAD_USERNAME="<your_nomad_username>"
NOMAD_PASSWORD="<your_nomad_password>"
```

You can access these explicitly with:


```python
NOMAD_USERNAME = environ('NOMAD_USERNAME')
NOMAD_PASSWORD = environ('NOMAD_PASSWORD')
NOMAD_USERNAME
```




    'JFRudzinski'



Use `get_authentication_token()` with your credentials to explicitly obtain and store a token:




```python
token = get_authentication_token(
    username=NOMAD_USERNAME, password=NOMAD_PASSWORD, url='test'
)
token
```

In practice, you do not need to obtain a token yourself when using nomad-utility-workflows. A token will automatically be obtained for API calls that require authentication. However, you may want to do the token generation yourself for custom API calls (see `Writing your own wrappers` below.)

### NOMAD User Metadata

nomad-utility-workflows uses the `NomadUser()` class to store the following user metadata:

```python
class NomadUser:
    user_id: str
    name: str
    first_name: str 
    last_name: str 
    username: str 
    affiliation: str 
    affiliation_address: str 
    email: str
    is_oasis_admin: bool 
    is_admin: bool
    repo_user_id: str 
    created: dt.datetime
```


You can retrieve your own personal info with the `who_am_i()` function:


```python
nomad_user_me = who_am_i(url='test')
nomad_user_me
```




    NomadUser(name='Joseph Rudzinski')



Similarly, you can query NOMAD for other users with `search_users_by_name()`:


```python
nomad_users = search_users_by_name('Rudzinski', url='test')
nomad_users
```




    [NomadUser(name='Joseph Rudzinski'), NomadUser(name='Joseph Rudzinski')]



In the case of multiple matches or for robustly identifying particular users, e.g., coauthors, in the future, it may be useful to store their `user_id`&mdash;a persistent identifier for each user account. Then, in the future you can use `get_user_by_id()` to grab the user info:


```python
nomad_user = get_user_by_id(nomad_users[0].user_id, url='test')
nomad_user
```




    NomadUser(name='Joseph Rudzinski')



### Uploading Data

nomad-utility-workflows uses the `NomadUpload()` class to store the following upload metadata:

```python
class NomadUpload:
    upload_id: str
    upload_create_time: dt.datetime
    main_author: NomadUser
    process_running: bool
    current_process: str
    process_status: str
    last_status_message: str
    errors: list[Any]
    warnings: list[Any]
    coauthors: list[str]
    coauthor_groups: list[Any]
    reviewers: list[NomadUser]
    reviewer_groups: list[Any]
    writers: list[NomadUser]
    writer_groups: list[Any]
    viewers: list[NomadUser]
    viewer_groups: list[Any]
    published: bool
    published_to: list[Any]
    with_embargo: bool
    embargo_length: float
    license: str
    entries: int
    n_entries: int
    upload_files_server_path: str
    publish_time: dt.datetime
    references: list[str] 
    datasets: list[str] 
    external_db: str 
    upload_name: str
    comment: str 
    url: str
    complete_time: dt.datetime
```

You can make an upload using the `upload_files_to_nomad()` function with input `filename=<path_to_a_zip_file_with_your_upload_data>`, as follows:  


```python
test_upload_fnm = (
    './test.zip'  # a dummy upload file containing a single empty json file
)
```


```python
upload_id = upload_files_to_nomad(filename=test_upload_fnm, url='test')
upload_id
```




    'e2b5o4KSR5yoS1EV9E9jXQ'



### Checking the upload status

The returned `upload_id` can then be used to directly access the upload, e.g., to check the upload status, using `get_upload_by_id()`:


```python
nomad_upload = get_upload_by_id(upload_id, url='test')

pprint(nomad_upload)
```

    NomadUpload(upload_id='e2b5o4KSR5yoS1EV9E9jXQ',
                upload_create_time=datetime.datetime(2024, 10, 16, 10, 14, 57, 722000),
                main_author=NomadUser(name='Joseph Rudzinski'),
                process_running=True,
                current_process='process_upload',
                process_status='RUNNING',
                last_status_message='Cleanup',
                errors=[],
                warnings=[],
                coauthors=[],
                coauthor_groups=[],
                reviewers=[],
                reviewer_groups=[],
                writers=[NomadUser(name='Joseph Rudzinski')],
                writer_groups=[],
                viewers=[NomadUser(name='Joseph Rudzinski')],
                viewer_groups=[],
                published=False,
                published_to=[],
                with_embargo=False,
                embargo_length=0.0,
                license='CC BY 4.0',
                entries=1,
                n_entries=None,
                upload_files_server_path='/nomad/test/fs/staging/e/e2b5o4KSR5yoS1EV9E9jXQ',
                publish_time=None,
                references=None,
                datasets=None,
                external_db=None,
                upload_name=None,
                comment=None,
                url='https://nomad-lab.eu/prod/v1/test/api/v1',
                complete_time=None)


One common usage of this function is to ensure that an upload has been processed successfully before making a subsequent action on it, e.g., editing the metadata or publishing. For this purpose, one could require the `process_running==False` or `process_status='SUCCESS'`, e.g.:

```python
    import time

    max_wait_time = 20 * 60  # 20 minutes in seconds
    interval = 2 * 60  # 2 minutes in seconds
    elapsed_time = 0

    while elapsed_time < max_wait_time:
        nomad_upload = get_upload_by_id(upload_id, url='test')
        
        # Check if the upload is complete
        if nomad_upload.process_status == 'SUCCESS':
            break
        
        # Wait for 2 minutes before the next call
        time.sleep(interval)
        elapsed_time += interval
    else:
        raise TimeoutError("Maximum wait time of 20 minutes exceeded. Upload is not complete.")
```

### Editing the upload metadata

After your upload is processed successfully, you can add coauthors, references, and other comments, as well as link to a dataset and provide a name for the upload. Note that the coauthor is specified by an email address that should correspond to the email linked to the person's NOMAD account, which can be accessed from `NomadUser.email`. The metadata should be stored as a dictionary as follows:

```python
metadata = {
    "metadata": {
    "upload_name": '<new_upload_name>',
    "references": ["https://doi.org/xx.xxxx/xxxxxx"],
    "datasets": '<dataset_id>',
    "embargo_length": 0,
    "coauthors": ["coauthor@affiliation.de"],
    "comment": 'This is a test upload...'
    },
}
```

For example:


```python
metadata_new = {'upload_name': 'Test Upload', 'comment': 'This is a test upload...'}
edit_upload_metadata(upload_id, url='test', upload_metadata=metadata_new)
```




    {'upload_id': 'e2b5o4KSR5yoS1EV9E9jXQ',
     'data': {'process_running': True,
      'current_process': 'edit_upload_metadata',
      'process_status': 'PENDING',
      'last_status_message': 'Pending: edit_upload_metadata',
      'errors': [],
      'warnings': [],
      'complete_time': '2024-10-16T10:14:58.322000',
      'upload_id': 'e2b5o4KSR5yoS1EV9E9jXQ',
      'upload_create_time': '2024-10-16T10:14:57.722000',
      'main_author': '8f052e1f-1906-41fd-b2eb-690c03407788',
      'coauthors': [],
      'coauthor_groups': [],
      'reviewers': [],
      'reviewer_groups': [],
      'writers': ['8f052e1f-1906-41fd-b2eb-690c03407788'],
      'writer_groups': [],
      'viewers': ['8f052e1f-1906-41fd-b2eb-690c03407788'],
      'viewer_groups': [],
      'published': False,
      'published_to': [],
      'with_embargo': False,
      'embargo_length': 0,
      'license': 'CC BY 4.0',
      'entries': 1,
      'upload_files_server_path': '/nomad/test/fs/staging/e/e2b5o4KSR5yoS1EV9E9jXQ'}}



Before moving on, let's again check that this additional process is complete:


```python
nomad_upload = get_upload_by_id(upload_id, url='test')

pprint(nomad_upload.process_status == 'SUCCESS')
pprint(nomad_upload.process_running is False)
```

    True
    True


### Accessing individual entries of an upload

During the upload process, NOMAD automatically identfies representative files that indicate the presence of data that can be parsed with the plugins included within a given deployment. This means that each upload can contain multiple *entries*&mdash;the fundamental unit storage within the NOMAD database.

You can query the individual entries within a known upload with `get_entries_of_upload()`, which then returns the metadata within the `NomadEntry()` class of nomad-utility-worklfows:

```python
class NomadEntry:
    entry_id: str
    upload_id: str
    references: list[str]
    origin: str
    quantities: list[str] 
    datasets: list[NomadDataset] 
    n_quantities: int
    nomad_version: str
    upload_create_time: dt.datetime
    nomad_commit: str
    section_defs: list[NomadSectionDefinition] 
    processing_errors: list[Any]
    results: dict
    entry_name: str
    last_processing_time: dt.datetime
    parser_name: str
    calc_id: str
    published: bool
    writers: list[NomadUser]
    sections: list[str] 
    processed: bool
    mainfile: str
    main_author: NomadUser
    viewers: list[NomadUser] 
    entry_create_time: dt.datetime
    with_embargo: bool
    files: list[str] 
    entry_type: str
    authors: list[NomadUser] 
    license: str
    domain: str
    optimade: dict
    comment: str
    upload_name: str
    viewer_groups: list[Any]
    writer_groups: list[Any]
    text_search_contents: list[str]
    publish_time: dt.datetime 
    entry_references: list[dict]
    url: str
```

Let's try this out with our test upload. In this case, the upload is *not* published and located in the *private* `Your Uploads` section of the NOMAD deployment. To access the uploads there, we need to set `with_authentication=True`:


```python
entries = get_entries_of_upload(upload_id, url='test', with_authentication=True)
pprint(f'Entries within upload_id={upload_id}:')
for entry in entries:
    pprint(f'entry_id={entry.entry_id}')
```

    'Entries within upload_id=e2b5o4KSR5yoS1EV9E9jXQ:'
    'entry_id=u4qPSfILguvJ9fabpklMTxbUJ7x2'


To query an entry directly using the `entry_id`, use `get_entry_by_id()`:


```python
entry = get_entry_by_id(entries[0].entry_id, url='test', with_authentication=True)
entry
```




    NomadEntry(entry_id='u4qPSfILguvJ9fabpklMTxbUJ7x2', upload_id='e2b5o4KSR5yoS1EV9E9jXQ', references=[], origin='Joseph Rudzinski', n_quantities=0, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 16, 10, 14, 57, 722000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 16, 10, 14, 58, 23000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='u4qPSfILguvJ9fabpklMTxbUJ7x2', published=False, writers=[NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 16, 10, 14, 57, 855000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment='This is a test upload...', upload_name='Test Upload', text_search_contents=[], publish_time=None, entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1')



You can download the full (meta)data stored in an entry using `download_entry_by_id()`. This will return the entire archive as a dictionary. If you supply a `zip_file_name` (including the desired local path), the raw data of the entry will also be downloaded and saved to a zip file. Otherwise, only the archive will be downloaded. 


```python
test = download_entry_by_id(
    entry.entry_id,
    url='test',
    zip_file_name='./raw_entry_data.zip',
    with_authentication=True,
)
test
```




    {'processing_logs': [{'event': 'Executing celery task',
       'proc': 'Entry',
       'process': 'process_entry',
       'process_worker_id': 'BOiybXorRqW5XImFf0SyoA',
       'parser': 'parsers/archive',
       'logger': 'nomad.processing',
       'timestamp': '2024-10-16 10:14.57',
       'level': 'DEBUG'},
      {'exec_time': '0.0013990402221679688',
       'input_size': '3',
       'event': 'parser executed',
       'proc': 'Entry',
       'process': 'process_entry',
       'process_worker_id': 'BOiybXorRqW5XImFf0SyoA',
       'parser': 'parsers/archive',
       'step': 'parsers/archive',
       'logger': 'nomad.processing',
       'timestamp': '2024-10-16 10:14.58',
       'level': 'INFO'},
      {'normalizer': 'MetainfoNormalizer',
       'step': 'MetainfoNormalizer',
       'event': 'normalizer completed successfully',
       'proc': 'Entry',
       'process': 'process_entry',
       'process_worker_id': 'BOiybXorRqW5XImFf0SyoA',
       'parser': 'parsers/archive',
       'logger': 'nomad.processing',
       'timestamp': '2024-10-16 10:14.58',
       'level': 'INFO'},
      {'exec_time': '0.0006549358367919922',
       'input_size': '3',
       'event': 'normalizer executed',
       'proc': 'Entry',
       'process': 'process_entry',
       'process_worker_id': 'BOiybXorRqW5XImFf0SyoA',
       'parser': 'parsers/archive',
       'normalizer': 'MetainfoNormalizer',
       'step': 'MetainfoNormalizer',
       'logger': 'nomad.processing',
       'timestamp': '2024-10-16 10:14.58',
       'level': 'INFO'},
      {'normalizer': 'ResultsNormalizer',
       'step': 'ResultsNormalizer',
       'event': 'normalizer completed successfully',
       'proc': 'Entry',
       'process': 'process_entry',
       'process_worker_id': 'BOiybXorRqW5XImFf0SyoA',
       'parser': 'parsers/archive',
       'logger': 'nomad.processing',
       'timestamp': '2024-10-16 10:14.58',
       'level': 'INFO'},
      {'exec_time': '0.0005123615264892578',
       'input_size': '3',
       'event': 'normalizer executed',
       'proc': 'Entry',
       'process': 'process_entry',
       'process_worker_id': 'BOiybXorRqW5XImFf0SyoA',
       'parser': 'parsers/archive',
       'normalizer': 'ResultsNormalizer',
       'step': 'ResultsNormalizer',
       'logger': 'nomad.processing',
       'timestamp': '2024-10-16 10:14.58',
       'level': 'INFO'},
      {'exec_time': '0.0015552043914794922',
       'event': 'entry metadata saved',
       'proc': 'Entry',
       'process': 'process_entry',
       'process_worker_id': 'BOiybXorRqW5XImFf0SyoA',
       'parser': 'parsers/archive',
       'logger': 'nomad.processing',
       'timestamp': '2024-10-16 10:14.58',
       'level': 'INFO'},
      {'exec_time': '0.09751391410827637',
       'event': 'entry metadata indexed',
       'proc': 'Entry',
       'process': 'process_entry',
       'process_worker_id': 'BOiybXorRqW5XImFf0SyoA',
       'parser': 'parsers/archive',
       'logger': 'nomad.processing',
       'timestamp': '2024-10-16 10:14.58',
       'level': 'INFO'}],
     'metadata': {'upload_id': 'e2b5o4KSR5yoS1EV9E9jXQ',
      'upload_create_time': '2024-10-16T10:14:57.722000+00:00',
      'entry_id': 'u4qPSfILguvJ9fabpklMTxbUJ7x2',
      'entry_name': 'test.archive.json',
      'entry_hash': 't6Zf68GLfrWxWRAIQu7QAY8LVmlL',
      'entry_create_time': '2024-10-16T10:14:57.855000+00:00',
      'parser_name': 'parsers/archive',
      'mainfile': 'test.archive.json',
      'text_search_contents': [],
      'files': ['test.archive.json'],
      'published': False,
      'with_embargo': False,
      'embargo_length': 0,
      'license': 'CC BY 4.0',
      'processed': True,
      'last_processing_time': '2024-10-16T10:14:58.023933+00:00',
      'processing_errors': [],
      'nomad_version': '1.3.7.dev55+ge83de27b3',
      'nomad_commit': '',
      'references': [],
      'main_author': '8f052e1f-1906-41fd-b2eb-690c03407788',
      'coauthors': [],
      'coauthor_groups': [],
      'entry_coauthors': [],
      'reviewers': [],
      'reviewer_groups': [],
      'datasets': [],
      'n_quantities': 34,
      'quantities': ['',
       'metadata',
       'metadata.coauthor_groups',
       'metadata.coauthors',
       'metadata.datasets',
       'metadata.embargo_length',
       'metadata.entry_coauthors',
       'metadata.entry_create_time',
       'metadata.entry_hash',
       'metadata.entry_id',
       'metadata.entry_name',
       'metadata.entry_timestamp',
       'metadata.entry_timestamp.timestamp',
       'metadata.entry_timestamp.token',
       'metadata.entry_timestamp.token_seed',
       'metadata.entry_timestamp.tsa_server',
       'metadata.files',
       'metadata.last_processing_time',
       'metadata.license',
       'metadata.main_author',
       'metadata.mainfile',
       'metadata.nomad_commit',
       'metadata.nomad_version',
       'metadata.parser_name',
       'metadata.processed',
       'metadata.processing_errors',
       'metadata.published',
       'metadata.quantities',
       'metadata.references',
       'metadata.reviewer_groups',
       'metadata.reviewers',
       'metadata.section_defs',
       'metadata.section_defs.definition_id',
       'metadata.section_defs.definition_qualified_name',
       'metadata.section_defs.used_directly',
       'metadata.sections',
       'metadata.upload_create_time',
       'metadata.upload_id',
       'metadata.with_embargo',
       'results',
       'results.properties'],
      'sections': ['nomad.datamodel.datamodel.EntryArchive',
       'nomad.datamodel.datamodel.EntryMetadata',
       'nomad.datamodel.datamodel.RFC3161Timestamp',
       'nomad.datamodel.results.Properties',
       'nomad.datamodel.results.Results'],
      'entry_timestamp': {'token_seed': 't6Zf68GLfrWxWRAIQu7QAY8LVmlL',
       'token': 'MIIERAYJKoZIhvcNAQcCoIIENTCCBDECAQMxDTALBglghkgBZQMEAgEwfQYLKoZIhvcNAQkQAQSgbgRsMGoCAQEGDCsGAQQBga0hgiwWATAvMAsGCWCGSAFlAwQCAQQgYnRB2tk2mTRtMamyedr2QFd3bb0lFM56N52xD8rv/OECFQDfPonFJHlhxHevtRkQ/Z85hZIntRgPMjAyNDEwMTYxMDE0NTdaMYIDnDCCA5gCAQEwgZ4wgY0xCzAJBgNVBAYTAkRFMUUwQwYDVQQKDDxWZXJlaW4genVyIEZvZXJkZXJ1bmcgZWluZXMgRGV1dHNjaGVuIEZvcnNjaHVuZ3NuZXR6ZXMgZS4gVi4xEDAOBgNVBAsMB0RGTi1QS0kxJTAjBgNVBAMMHERGTi1WZXJlaW4gR2xvYmFsIElzc3VpbmcgQ0ECDCkC1XMzD3ji9J60uTALBglghkgBZQMEAgGggdEwGgYJKoZIhvcNAQkDMQ0GCyqGSIb3DQEJEAEEMBwGCSqGSIb3DQEJBTEPFw0yNDEwMTYxMDE0NTdaMCsGCSqGSIb3DQEJNDEeMBwwCwYJYIZIAWUDBAIBoQ0GCSqGSIb3DQEBCwUAMC8GCSqGSIb3DQEJBDEiBCA6s2JcwOqwu7132Qqq//qLCZ/RvK+vbG2KEmFaDWTOezA3BgsqhkiG9w0BCRACLzEoMCYwJDAiBCC2CI293QiY00kHjXwjMqdOzIQUDKCDWfAjVyVGz26C5DANBgkqhkiG9w0BAQsFAASCAgBq9WFjtr/M3uah2LM9SuT7Mg/clX91fkKHk+/a+Y4gZGzOGdeE4P3AlHKDsNmOqghgvX7EVXgMadc+Hyf72YGNaqrca3BFYivuI4NqbjX7+wC9vq0SZIn+5d3TBCCZisZmAtZHTj7+KZpD071qGLMmsaCi+qOTQExgB0G7DRYeGibq3slZXKTr2wY8frQDB6M36y69nPgovO4azvy+553w2EQQkAGfemSxz78S9A2jcV9frKxOIGXUb5R/FVYGfJfCM/C+d35iUGisi9ZWHl0PvSe2Zzwv+4UQRr4ljEGrnfDkdKrV1ZluxerBNrzxcYU+DpWUPs8NZdfQibSkmc9aMwvznrqB4GL3iW+8RrQrGSgiIm84HKlYp6TNCWldlGlV3Xfgo+6AO3hZv5v7tiM7GWLL6/fFi4IRPaIGCldL9D0Yun6U95vaiy+jRT1yzZx4pPlgM3JbSp4z/BkmTURBhHySUsr91LkB8OAiS5LKEZye7qJTuBdI9Ny7TNB050c5+mmfv/TKmbExIZoYvChXQKCUMCCLLSFmxN4eoVvwULziEaJd+d6e94QoEJjc3Queb/15zT0yP6X/alokIVhi3AhkHUwkFWCJlg/d9UsdiXZYe3JfzAHxwslS204ohwSQ6NhSvKIUN1ybb19ChR+QqA1JEyv33DKGAr5BPpN72A==',
       'tsa_server': 'http://zeitstempel.dfn.de',
       'timestamp': '2024-10-16T10:14:57+00:00'},
      'section_defs': [{'definition_qualified_name': 'nomad.datamodel.data.ArchiveSection',
        'definition_id': '7047cbff9980abff17cce4b1b6b0d1c783505b7f',
        'used_directly': True},
       {'definition_qualified_name': 'nomad.datamodel.datamodel.EntryArchive',
        'definition_id': '510c3beb8699d7d23a29bb0cf45540286916c20c',
        'used_directly': True},
       {'definition_qualified_name': 'nomad.datamodel.datamodel.EntryMetadata',
        'definition_id': '6edfa503af63b84d6a6021c227d00137b4c1cc9c',
        'used_directly': True},
       {'definition_qualified_name': 'nomad.datamodel.datamodel.RFC3161Timestamp',
        'definition_id': '1e3e9dd7b802b04343f46305a7d0f58663d8110a',
        'used_directly': True},
       {'definition_qualified_name': 'nomad.datamodel.results.Properties',
        'definition_id': '3d0188853e1806435f95f9a876b83ed98ad38713',
        'used_directly': True},
       {'definition_qualified_name': 'nomad.datamodel.results.Results',
        'definition_id': '1caea35fada02e0b6861ceec2dd928595fc824db',
        'used_directly': True}]},
     'results': {'properties': {}},
     'm_ref_archives': {}}



## Publishing Uploads

Once the processing of your upload is successful and you have added/adjusted the appropriate metadata, you can publish your upload with `publish_upload()`, making it publicly available on the corresponding NOMAD deployment. 

Note that once the upload is published you will no longer be able to make changes to the raw files that you uploaded. However, the upload metadata (accessed and edited in the above example) can be changed after publishing.


```python
published_upload = publish_upload(nomad_upload.upload_id, url='test')
published_upload
```




    {'upload_id': 'e2b5o4KSR5yoS1EV9E9jXQ',
     'data': {'process_running': True,
      'current_process': 'publish_upload',
      'process_status': 'PENDING',
      'last_status_message': 'Pending: publish_upload',
      'errors': [],
      'warnings': [],
      'complete_time': '2024-10-16T10:14:59.363000',
      'upload_id': 'e2b5o4KSR5yoS1EV9E9jXQ',
      'upload_name': 'Test Upload',
      'upload_create_time': '2024-10-16T10:14:57.722000',
      'main_author': '8f052e1f-1906-41fd-b2eb-690c03407788',
      'coauthors': [],
      'coauthor_groups': [],
      'reviewers': [],
      'reviewer_groups': [],
      'writers': ['8f052e1f-1906-41fd-b2eb-690c03407788'],
      'writer_groups': [],
      'viewers': ['8f052e1f-1906-41fd-b2eb-690c03407788'],
      'viewer_groups': [],
      'published': False,
      'published_to': [],
      'with_embargo': False,
      'embargo_length': 0,
      'license': 'CC BY 4.0',
      'entries': 1,
      'upload_files_server_path': '/nomad/test/fs/staging/e/e2b5o4KSR5yoS1EV9E9jXQ'}}



## Finding and Creating Datasets

Although uploads can group multiple entries together, they are limited by the maximum upload size and act more as a practical tool for optimizing the transfer of data to the NOMAD repository. For scientifically relevant connections between entries, NOMAD uses *Datasets* and *Workflows*. 

You can easily create a dataset with `create_dataset()`:



```python
dataset_id = create_dataset('test dataset', url='test')
dataset_id
```




    'EfhadCxpRaGpw50rWzK22w'



The returned `dataset_id` can then be used to add individual entries (or all entries within an upload) to the dataset by including it in the upload/entry metadata, using the method described above:


```python
metadata_new = {'dataset_id': dataset_id}
edit_upload_metadata(upload_id, url='test', upload_metadata=metadata_new)
```




    {'upload_id': 'e2b5o4KSR5yoS1EV9E9jXQ',
     'data': {'process_running': True,
      'current_process': 'edit_upload_metadata',
      'process_status': 'PENDING',
      'last_status_message': 'Pending: edit_upload_metadata',
      'errors': [],
      'warnings': [],
      'complete_time': '2024-10-16T10:15:04.112000',
      'upload_id': 'e2b5o4KSR5yoS1EV9E9jXQ',
      'upload_name': 'Test Upload',
      'upload_create_time': '2024-10-16T10:14:57.722000',
      'main_author': '8f052e1f-1906-41fd-b2eb-690c03407788',
      'coauthors': [],
      'coauthor_groups': [],
      'reviewers': [],
      'reviewer_groups': [],
      'writers': ['8f052e1f-1906-41fd-b2eb-690c03407788'],
      'writer_groups': [],
      'viewers': ['8f052e1f-1906-41fd-b2eb-690c03407788'],
      'viewer_groups': [],
      'published': True,
      'published_to': [],
      'publish_time': '2024-10-16T10:15:04.099000',
      'with_embargo': False,
      'embargo_length': 0,
      'license': 'CC BY 4.0',
      'entries': 1}}




```python
nomad_upload = get_upload_by_id(upload_id, url='test')

pprint(nomad_upload.process_status == 'SUCCESS')
pprint(nomad_upload.process_running is False)
```

    True
    True




You can also retrieve the dataset metadata using the `dataset_id` with `get_dataset_by_id()`. The returned `NomadDataset()` class contains the following attributes:

```python
class NomadDataset:
    dataset_id: str
    dataset_create_time: dt.datetime
    dataset_name: str
    dataset_type: str
    dataset_modified_time: dt.datetime
    user: NomadUser
    doi: str
    pid: int
    m_annotations: dict
```


```python
nomad_dataset = get_dataset_by_id(dataset_id, url='test')
nomad_dataset
```




    NomadDataset(dataset_id='EfhadCxpRaGpw50rWzK22w', dataset_create_time=datetime.datetime(2024, 10, 16, 10, 15, 5, 240000), dataset_name='test dataset', dataset_type='owned', dataset_modified_time=datetime.datetime(2024, 10, 16, 10, 15, 5, 240000), user=NomadUser(name='Joseph Rudzinski'), doi=None, pid=None, m_annotations=None)



Alternatively, you can search for datasets, e.g., by `user_id` or `dataset_name`, using `retrieve_datasets()`:


```python
my_datasets = retrieve_datasets(
    dataset_params={'user_id': nomad_user_me.user_id, 'max_datasets': 20}, url='test'
)
pprint(my_datasets)
```

    [NomadDataset(dataset_id='EfhadCxpRaGpw50rWzK22w',
                  dataset_create_time=datetime.datetime(2024, 10, 16, 10, 15, 5, 240000),
                  dataset_name='test dataset',
                  dataset_type='owned',
                  dataset_modified_time=datetime.datetime(2024, 10, 16, 10, 15, 5, 240000),
                  user=NomadUser(name='Joseph Rudzinski'),
                  doi=None,
                  pid=None,
                  m_annotations=None)]


To get the list of entries contained within a dataset, use `query_entries()`:


```python
dataset_entries = query_entries(query_params={'dataset_id': dataset_id}, url='test')
for entry in dataset_entries:
    pprint(f'entry_id={entry.entry_id}, upload_id={entry.upload_id}')
```

    'entry_id=u4qPSfILguvJ9fabpklMTxbUJ7x2, upload_id=e2b5o4KSR5yoS1EV9E9jXQ'


There is no "publishing" action for datasets. Instead, when the dataset is complete (i.e., you are ready to lock the contents of the dataset), you can *assign a DOI*. There is currently no API action for this within nomad-utility-workflows. You must go to the GUI of the relevant deployment, go to `PUBLISH > Datasets`, find the dataset, and then click the "assign a DOI" banner icon to the right of the dataset entry.

## Deleting Uploads and Datasets

You can delete uploads and datasets using `delete_upload()` and `delete_dataset()` as demonstrated in the following examples (along with the previously explained workflow of uploading, editing, etc.). Note that the wait times in these examples are arbitrary. One should optimize these for specific use cases.

**upload, check for success, delete, check for success**:


```python
# Make a dummy upload
upload_id = upload_files_to_nomad(filename=test_upload_fnm, url='test')


max_wait_time = 15  # 15 seconds
interval = 5  # 5 seconds
elapsed_time = 0

while elapsed_time < max_wait_time:
    # Get the upload
    nomad_upload = get_upload_by_id(upload_id, url='test')

    # Check if the upload is complete
    if nomad_upload.process_status == 'SUCCESS':
        break

    # Wait for 5 seconds before the next call
    time.sleep(interval)
    elapsed_time += interval
else:
    raise TimeoutError(
        'Maximum wait time of 15 seconds exceeded. Upload is not complete.'
    )

# Delete the upload
delete_upload(upload_id, url='test')

# Wait for 5 seconds to make sure deletion is complete
time.sleep(5)

# Check if the upload was deleted
try:
    get_upload_by_id(upload_id, url='test')
except Exception:
    pprint(f'Upload with upload_id={upload_id} was deleted successfully.')
```

    'Upload with upload_id=Mfp1OnhMSY65eZCfNvS8DA was deleted successfully.'


**create dataset, check for success, delete, check for success**:


```python
# Make a dummy dataset
dataset_id = create_dataset('dummy dataset', url='test')

# Wait for 5 seconds to make sure dataset is created
time.sleep(5)

# Ensure the dataset was created
dummy_dataset = get_dataset_by_id(dataset_id, url='test')
assert dummy_dataset.dataset_id == dataset_id

# Delete the upload
delete_dataset(dataset_id, url='test')

# Wait for 5 seconds to make sure deletion is complete
time.sleep(5)

# Check if the dataset was deleted
try:
    get_dataset_by_id(dataset_id, url='test')
except Exception:
    pprint(f'Dataset with dataset_id={dataset_id} was deleted successfully.')
```

    'Dataset with dataset_id=Cke5DQkdQ0qbLOky2zGfLw was deleted successfully.'


## Useful Wrappers

nomad-utility-workflows contains a few useful wrapper functions to help users query all of their uploads and corresponding entries:


```python
get_all_my_uploads(url='test')
```




    [NomadUpload(upload_id='bQa5SGDQQ8auQUBb5AaYHw', upload_create_time=datetime.datetime(2024, 10, 14, 10, 48, 40, 994000), main_author=NomadUser(name='Joseph Rudzinski'), process_running=False, current_process='publish_upload', process_status='SUCCESS', last_status_message='Process publish_upload completed successfully', errors=[], warnings=[], coauthors=[], coauthor_groups=[], reviewers=[], reviewer_groups=[], writers=[NomadUser(name='Joseph Rudzinski')], writer_groups=[], viewers=[NomadUser(name='Joseph Rudzinski')], viewer_groups=[], published=True, published_to=[], with_embargo=False, embargo_length=0.0, license='CC BY 4.0', entries=1, n_entries=None, upload_files_server_path=None, publish_time=datetime.datetime(2024, 10, 14, 10, 48, 55, 806000), references=None, datasets=None, external_db=None, upload_name='Test Upload', comment=None, url='https://nomad-lab.eu/prod/v1/test/api/v1', complete_time=datetime.datetime(2024, 10, 14, 10, 48, 55, 818000)),
     NomadUpload(upload_id='DN61X4r7SCyzm5q1kxcEcw', upload_create_time=datetime.datetime(2024, 10, 14, 10, 55, 12, 410000), main_author=NomadUser(name='Joseph Rudzinski'), process_running=False, current_process='publish_upload', process_status='SUCCESS', last_status_message='Process publish_upload completed successfully', errors=[], warnings=[], coauthors=[], coauthor_groups=[], reviewers=[], reviewer_groups=[], writers=[NomadUser(name='Joseph Rudzinski')], writer_groups=[], viewers=[NomadUser(name='Joseph Rudzinski')], viewer_groups=[], published=True, published_to=[], with_embargo=False, embargo_length=0.0, license='CC BY 4.0', entries=1, n_entries=None, upload_files_server_path=None, publish_time=datetime.datetime(2024, 10, 14, 10, 55, 23, 52000), references=None, datasets=None, external_db=None, upload_name='Test Upload', comment=None, url='https://nomad-lab.eu/prod/v1/test/api/v1', complete_time=datetime.datetime(2024, 10, 14, 10, 55, 23, 65000)),
     NomadUpload(upload_id='z4QvhZ7qSCmgIFv_qJqlyQ', upload_create_time=datetime.datetime(2024, 10, 14, 20, 20, 38, 757000), main_author=NomadUser(name='Joseph Rudzinski'), process_running=False, current_process='edit_upload_metadata', process_status='SUCCESS', last_status_message='Process edit_upload_metadata completed successfully', errors=[], warnings=[], coauthors=['7c85bdf1-8b53-40a8-81a4-04f26ff56f29'], coauthor_groups=[], reviewers=[], reviewer_groups=[], writers=[NomadUser(name='Joseph Rudzinski'), NomadUser(name='Joseph Rudzinski')], writer_groups=[], viewers=[NomadUser(name='Joseph Rudzinski'), NomadUser(name='Joseph Rudzinski')], viewer_groups=[], published=True, published_to=[], with_embargo=False, embargo_length=0.0, license='CC BY 4.0', entries=1, n_entries=None, upload_files_server_path=None, publish_time=datetime.datetime(2024, 10, 15, 6, 18, 27, 700000), references=None, datasets=None, external_db=None, upload_name='Test Upload', comment=None, url='https://nomad-lab.eu/prod/v1/test/api/v1', complete_time=datetime.datetime(2024, 10, 15, 6, 22, 33, 45000)),
     NomadUpload(upload_id='GJdVAOCxRVe-Cwo3qMz9Kg', upload_create_time=datetime.datetime(2024, 10, 15, 10, 48, 44, 337000), main_author=NomadUser(name='Joseph Rudzinski'), process_running=False, current_process='edit_upload_metadata', process_status='SUCCESS', last_status_message='Process edit_upload_metadata completed successfully', errors=[], warnings=[], coauthors=[], coauthor_groups=[], reviewers=[], reviewer_groups=[], writers=[NomadUser(name='Joseph Rudzinski')], writer_groups=[], viewers=[NomadUser(name='Joseph Rudzinski')], viewer_groups=[], published=True, published_to=[], with_embargo=False, embargo_length=0.0, license='CC BY 4.0', entries=1, n_entries=None, upload_files_server_path=None, publish_time=datetime.datetime(2024, 10, 15, 10, 49, 24, 4000), references=None, datasets=None, external_db=None, upload_name='Test Upload', comment=None, url='https://nomad-lab.eu/prod/v1/test/api/v1', complete_time=datetime.datetime(2024, 10, 15, 10, 49, 30, 962000)),
     NomadUpload(upload_id='RdA_3ZsOTMqbtAhYLivVsw', upload_create_time=datetime.datetime(2024, 10, 15, 20, 2, 10, 378000), main_author=NomadUser(name='Joseph Rudzinski'), process_running=False, current_process='edit_upload_metadata', process_status='SUCCESS', last_status_message='Process edit_upload_metadata completed successfully', errors=[], warnings=[], coauthors=[], coauthor_groups=[], reviewers=[], reviewer_groups=[], writers=[NomadUser(name='Joseph Rudzinski')], writer_groups=[], viewers=[NomadUser(name='Joseph Rudzinski')], viewer_groups=[], published=True, published_to=[], with_embargo=False, embargo_length=0.0, license='CC BY 4.0', entries=1, n_entries=None, upload_files_server_path=None, publish_time=datetime.datetime(2024, 10, 15, 20, 9, 28, 757000), references=None, datasets=None, external_db=None, upload_name='Test Upload', comment=None, url='https://nomad-lab.eu/prod/v1/test/api/v1', complete_time=datetime.datetime(2024, 10, 15, 20, 10, 33, 141000)),
     NomadUpload(upload_id='8vViZoL3TYG9fMFibPkjlw', upload_create_time=datetime.datetime(2024, 10, 16, 9, 25, 53, 929000), main_author=NomadUser(name='Joseph Rudzinski'), process_running=False, current_process='publish_upload', process_status='SUCCESS', last_status_message='Process publish_upload completed successfully', errors=[], warnings=[], coauthors=[], coauthor_groups=[], reviewers=[], reviewer_groups=[], writers=[NomadUser(name='Joseph Rudzinski')], writer_groups=[], viewers=[NomadUser(name='Joseph Rudzinski')], viewer_groups=[], published=True, published_to=[], with_embargo=False, embargo_length=0.0, license='CC BY 4.0', entries=1, n_entries=None, upload_files_server_path=None, publish_time=datetime.datetime(2024, 10, 16, 9, 43, 18, 243000), references=None, datasets=None, external_db=None, upload_name='Test Upload', comment=None, url='https://nomad-lab.eu/prod/v1/test/api/v1', complete_time=datetime.datetime(2024, 10, 16, 9, 43, 18, 255000)),
     NomadUpload(upload_id='cP4q5rRsQM-D60Tp3olPdQ', upload_create_time=datetime.datetime(2024, 10, 16, 9, 47, 31, 721000), main_author=NomadUser(name='Joseph Rudzinski'), process_running=False, current_process='publish_upload', process_status='SUCCESS', last_status_message='Process publish_upload completed successfully', errors=[], warnings=[], coauthors=[], coauthor_groups=[], reviewers=[], reviewer_groups=[], writers=[NomadUser(name='Joseph Rudzinski')], writer_groups=[], viewers=[NomadUser(name='Joseph Rudzinski')], viewer_groups=[], published=True, published_to=[], with_embargo=False, embargo_length=0.0, license='CC BY 4.0', entries=1, n_entries=None, upload_files_server_path=None, publish_time=datetime.datetime(2024, 10, 16, 9, 47, 46, 247000), references=None, datasets=None, external_db=None, upload_name='Test Upload', comment=None, url='https://nomad-lab.eu/prod/v1/test/api/v1', complete_time=datetime.datetime(2024, 10, 16, 9, 47, 46, 257000)),
     NomadUpload(upload_id='5ADf3M4uSByqsYpkEB6UEg', upload_create_time=datetime.datetime(2024, 10, 16, 9, 52, 1, 469000), main_author=NomadUser(name='Joseph Rudzinski'), process_running=False, current_process='process_upload', process_status='SUCCESS', last_status_message='Process process_upload completed successfully', errors=[], warnings=[], coauthors=[], coauthor_groups=[], reviewers=[], reviewer_groups=[], writers=[NomadUser(name='Joseph Rudzinski')], writer_groups=[], viewers=[NomadUser(name='Joseph Rudzinski')], viewer_groups=[], published=False, published_to=[], with_embargo=False, embargo_length=0.0, license='CC BY 4.0', entries=1, n_entries=None, upload_files_server_path='/nomad/test/fs/staging/5/5ADf3M4uSByqsYpkEB6UEg', publish_time=None, references=None, datasets=None, external_db=None, upload_name=None, comment=None, url='https://nomad-lab.eu/prod/v1/test/api/v1', complete_time=datetime.datetime(2024, 10, 16, 9, 52, 2, 94000)),
     NomadUpload(upload_id='_mZn0RZ8QtmBkcAlPU5bSw', upload_create_time=datetime.datetime(2024, 10, 16, 9, 52, 47, 649000), main_author=NomadUser(name='Joseph Rudzinski'), process_running=False, current_process='edit_upload_metadata', process_status='SUCCESS', last_status_message='Process edit_upload_metadata completed successfully', errors=[], warnings=[], coauthors=[], coauthor_groups=[], reviewers=[], reviewer_groups=[], writers=[NomadUser(name='Joseph Rudzinski')], writer_groups=[], viewers=[NomadUser(name='Joseph Rudzinski')], viewer_groups=[], published=True, published_to=[], with_embargo=False, embargo_length=0.0, license='CC BY 4.0', entries=1, n_entries=None, upload_files_server_path=None, publish_time=datetime.datetime(2024, 10, 16, 9, 53, 0, 835000), references=None, datasets=None, external_db=None, upload_name='Test Upload', comment=None, url='https://nomad-lab.eu/prod/v1/test/api/v1', complete_time=datetime.datetime(2024, 10, 16, 9, 53, 4, 791000)),
     NomadUpload(upload_id='Cntk6OsQTvaZp7r6Jom-3g', upload_create_time=datetime.datetime(2024, 10, 16, 9, 54, 47, 426000), main_author=NomadUser(name='Joseph Rudzinski'), process_running=False, current_process='edit_upload_metadata', process_status='SUCCESS', last_status_message='Process edit_upload_metadata completed successfully', errors=[], warnings=[], coauthors=[], coauthor_groups=[], reviewers=[], reviewer_groups=[], writers=[NomadUser(name='Joseph Rudzinski')], writer_groups=[], viewers=[NomadUser(name='Joseph Rudzinski')], viewer_groups=[], published=True, published_to=[], with_embargo=False, embargo_length=0.0, license='CC BY 4.0', entries=1, n_entries=None, upload_files_server_path=None, publish_time=datetime.datetime(2024, 10, 16, 9, 54, 51, 988000), references=None, datasets=None, external_db=None, upload_name='Test Upload', comment=None, url='https://nomad-lab.eu/prod/v1/test/api/v1', complete_time=datetime.datetime(2024, 10, 16, 9, 54, 53, 838000))]




```python
get_entries_of_my_uploads(url='test')
```




    [NomadEntry(entry_id='ycdeXhPDG-nIgEQlqBfzIEKPWCvy', upload_id='bQa5SGDQQ8auQUBb5AaYHw', references=[], origin='Joseph Rudzinski', n_quantities=34, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 14, 10, 48, 40, 994000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 14, 10, 48, 42, 415000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='ycdeXhPDG-nIgEQlqBfzIEKPWCvy', published=True, writers=[NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 14, 10, 48, 41, 672000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment='This is a test upload...', upload_name='Test Upload', text_search_contents=[], publish_time=None, entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1'),
     NomadEntry(entry_id='7A6lJb-14xR9lxXO8kjuYt5-vxg2', upload_id='DN61X4r7SCyzm5q1kxcEcw', references=[], origin='Joseph Rudzinski', n_quantities=34, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 14, 10, 55, 12, 410000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 14, 10, 55, 12, 808000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='7A6lJb-14xR9lxXO8kjuYt5-vxg2', published=True, writers=[NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 14, 10, 55, 12, 563000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment='This is a test upload...', upload_name='Test Upload', text_search_contents=[], publish_time=None, entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1'),
     NomadEntry(entry_id='jWSpYURP5GgPtgF9LXZJpNlDv-GL', upload_id='z4QvhZ7qSCmgIFv_qJqlyQ', references=[], origin='Joseph Rudzinski', n_quantities=0, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 14, 20, 20, 38, 757000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 14, 20, 20, 39, 272000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='jWSpYURP5GgPtgF9LXZJpNlDv-GL', published=True, writers=[NomadUser(name='Joseph Rudzinski'), NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 14, 20, 20, 38, 982000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment='This is a test upload...edited', upload_name='Test Upload', text_search_contents=[], publish_time=datetime.datetime(2024, 10, 15, 6, 18, 27, 700000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1'),
     NomadEntry(entry_id='MVBIMEZOuIzH7-QFU2TtMIM6LLPp', upload_id='GJdVAOCxRVe-Cwo3qMz9Kg', references=[], origin='Joseph Rudzinski', n_quantities=0, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 15, 10, 48, 44, 337000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 15, 10, 48, 45, 206000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='MVBIMEZOuIzH7-QFU2TtMIM6LLPp', published=True, writers=[NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 15, 10, 48, 44, 741000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment='This is a test upload...', upload_name='Test Upload', text_search_contents=[], publish_time=datetime.datetime(2024, 10, 15, 10, 49, 24, 4000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1'),
     NomadEntry(entry_id='Htbl78lHDSNAKbvPjEgEN_6sOcxF', upload_id='RdA_3ZsOTMqbtAhYLivVsw', references=[], origin='Joseph Rudzinski', n_quantities=0, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 15, 20, 2, 10, 378000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 15, 20, 2, 10, 752000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='Htbl78lHDSNAKbvPjEgEN_6sOcxF', published=True, writers=[NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 15, 20, 2, 10, 543000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment='This is a test upload...', upload_name='Test Upload', text_search_contents=[], publish_time=datetime.datetime(2024, 10, 15, 20, 9, 28, 757000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1'),
     NomadEntry(entry_id='zyaF373NIH-igHS3TZN5FW4SaO4d', upload_id='8vViZoL3TYG9fMFibPkjlw', references=[], origin='Joseph Rudzinski', n_quantities=34, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 16, 9, 25, 53, 929000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 16, 9, 25, 54, 274000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='zyaF373NIH-igHS3TZN5FW4SaO4d', published=True, writers=[NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 16, 9, 25, 54, 111000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment='This is a test upload...', upload_name='Test Upload', text_search_contents=[], publish_time=None, entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1'),
     NomadEntry(entry_id='Xitkh3ZVhRu11LUIk5n0cA2Wtmmy', upload_id='cP4q5rRsQM-D60Tp3olPdQ', references=[], origin='Joseph Rudzinski', n_quantities=34, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 16, 9, 47, 31, 721000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 16, 9, 47, 32, 28000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='Xitkh3ZVhRu11LUIk5n0cA2Wtmmy', published=True, writers=[NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 16, 9, 47, 31, 857000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment='This is a test upload...', upload_name='Test Upload', text_search_contents=[], publish_time=None, entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1'),
     NomadEntry(entry_id='EaFsd7Ku9IEXOcwTcMqlWd92fZx1', upload_id='5ADf3M4uSByqsYpkEB6UEg', references=[], origin='Joseph Rudzinski', n_quantities=34, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 16, 9, 52, 1, 469000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 16, 9, 52, 1, 752000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='EaFsd7Ku9IEXOcwTcMqlWd92fZx1', published=False, writers=[NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 16, 9, 52, 1, 595000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment=None, upload_name=None, text_search_contents=[], publish_time=None, entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1'),
     NomadEntry(entry_id='h3e0Z5FHiUetLmW8kbPW4uwrT0gH', upload_id='_mZn0RZ8QtmBkcAlPU5bSw', references=[], origin='Joseph Rudzinski', n_quantities=0, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 16, 9, 52, 47, 649000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 16, 9, 52, 47, 948000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='h3e0Z5FHiUetLmW8kbPW4uwrT0gH', published=True, writers=[NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 16, 9, 52, 47, 780000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment='This is a test upload...', upload_name='Test Upload', text_search_contents=[], publish_time=datetime.datetime(2024, 10, 16, 9, 53, 0, 835000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1'),
     NomadEntry(entry_id='lJiZnALI0ad8UKh5nt2FG1rhaZiC', upload_id='Cntk6OsQTvaZp7r6Jom-3g', references=[], origin='Joseph Rudzinski', n_quantities=0, nomad_version='1.3.7.dev55+ge83de27b3', upload_create_time=datetime.datetime(2024, 10, 16, 9, 54, 47, 426000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), nomad_commit='', processing_errors=[], entry_name='test.archive.json', last_processing_time=datetime.datetime(2024, 10, 16, 9, 54, 47, 719000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), parser_name='parsers/archive', calc_id='lJiZnALI0ad8UKh5nt2FG1rhaZiC', published=True, writers=[NomadUser(name='Joseph Rudzinski')], processed=True, mainfile='test.archive.json', main_author=NomadUser(name='Joseph Rudzinski'), entry_create_time=datetime.datetime(2024, 10, 16, 9, 54, 47, 553000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), with_embargo=False, entry_type=None, license='CC BY 4.0', domain=None, comment='This is a test upload...', upload_name='Test Upload', text_search_contents=[], publish_time=datetime.datetime(2024, 10, 16, 9, 54, 51, 988000, tzinfo=datetime.timezone(datetime.timedelta(0), '+0000')), entry_references=None, url='https://nomad-lab.eu/prod/v1/test/api/v1')]



### Writing Your Own Wrappers

In `nomad_utility_workflows.utils.core` you will find the core NOMAD API functions `get_nomad_request()`, `post_nomad_request()`, and `delete_nomad_request()`. Using these as a basis, along with the [NOMAD API Dashboard](https://nomad-lab.eu/prod/v1/staging/api/v1/extensions/docs#/), you can easily extend the `nomad-utility-workflows` module for making more specific queries within your specialized workflows.

