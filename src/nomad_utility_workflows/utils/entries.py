import datetime as dt
import json
import logging
import os
import shutil
from dataclasses import field
from pathlib import Path
from typing import Any, ByteString, Optional, cast
from zipfile import ZipFile

from cachetools.func import ttl_cache
from marshmallow import Schema, pre_load, EXCLUDE
from marshmallow_dataclass import class_schema, dataclass
# from signac.job import Job

# from martignac import config
from nomad_utility_workflows.utils.datasets import NomadDataset
from nomad_utility_workflows.utils.uploads import get_all_my_uploads
from nomad_utility_workflows.utils.users import NomadUser, get_user_by_id
from nomad_utility_workflows.utils.utils import (
    get_nomad_url,
    get_nomad_url_name,
    get_nomad_base_url,
    get_nomad_request,
    post_nomad_request,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NomadSectionDefinition:
    used_directly: bool
    definition_id: str
    definition_qualified_name: str


class NomadEntrySchema(Schema):
    # class Meta:
    #     unknown = EXCLUDE
    @pre_load
    def convert_users(self, data, **kwargs):
        data['main_author'] = get_user_by_id(
            user_id=data['main_author']['user_id']
        ).as_dict()
        data['writers'] = [
            get_user_by_id(user_id=w['user_id']).as_dict() for w in data['writers']
        ]
        data['authors'] = [
            get_user_by_id(user_id=a['user_id']).as_dict() for a in data['authors']
        ]
        data['viewers'] = [
            get_user_by_id(user_id=v['user_id']).as_dict() for v in data['viewers']
        ]
        return data


# ? Should we just make these all optional to avoid the get functions breaking?
@dataclass(frozen=True)
class NomadEntry:
    entry_id: str
    upload_id: str
    references: list[str]
    origin: str
    quantities: list[str] = field(repr=False)
    datasets: list[NomadDataset] = field(repr=False)
    n_quantities: int
    nomad_version: str
    upload_create_time: dt.datetime
    nomad_commit: str
    section_defs: list[NomadSectionDefinition] = field(repr=False)
    processing_errors: list[Any]
    results: dict = field(repr=False)
    entry_name: str
    last_processing_time: dt.datetime
    parser_name: str
    calc_id: str
    published: bool
    writers: list[NomadUser]
    sections: list[str] = field(repr=False)
    processed: bool
    mainfile: str
    main_author: NomadUser
    viewers: list[NomadUser] = field(repr=False)
    entry_create_time: dt.datetime
    with_embargo: bool
    files: list[str] = field(repr=False)
    entry_type: Optional[str]
    authors: list[NomadUser] = field(repr=False)
    license: str
    domain: Optional[str] = None
    optimade: Optional[dict] = field(repr=False, default=None)
    comment: Optional[str] = None
    upload_name: Optional[str] = None
    viewer_groups: Optional[list[Any]] = field(repr=False, default=None)
    writer_groups: Optional[list[Any]] = field(repr=False, default=None)
    text_search_contents: Optional[list[str]] = None
    publish_time: Optional[dt.datetime] = None
    entry_references: Optional[list[dict]] = None
    url: Optional[str] = None

    @property
    def base_url(self) -> Optional[str]:
        url = get_nomad_url(self.url)
        return get_nomad_base_url(url)

    @property
    def nomad_gui_url(self) -> str:
        if self.upload_id is None or self.entry_id is None:
            raise ValueError(
                f"missing attributes 'upload_id' or 'entry_id' for entry {self}"
            )
        return f'{self.base_url}/gui/user/uploads/upload/id/{self.upload_id}/entry/id/{self.entry_id}'

    @property
    def job_id(self) -> Optional[str]:
        return self._comment_dict.get('job_id', None)

    @property
    def workflow_name(self) -> Optional[str]:
        return self._comment_dict.get('workflow_name', None)

    @property
    def state_point(self) -> dict:
        return self._comment_dict.get('state_point', {})

    @property
    def mdp_files(self) -> Optional[str]:
        return self._comment_dict.get('mdp_files', None)

    @property
    def _comment_dict(self) -> dict:
        return json.loads(self.comment or '{}')


@ttl_cache(maxsize=128, ttl=180)
def get_entry_by_id(
    entry_id: str,
    url: str = None,
    with_authentication: bool = False,
    timeout_in_sec: int = 10,
) -> NomadEntry:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('retrieving entry %s on %s server', entry_id, url_name)
    response = get_nomad_request(
        f'/entries/{entry_id}',
        with_authentication=with_authentication,
        url=url,
        timeout_in_sec=timeout_in_sec,
    )
    nomad_entry_schema = class_schema(NomadEntry, base_schema=NomadEntrySchema)
    return nomad_entry_schema().load({**response['data'], 'url': url})


@ttl_cache(maxsize=128, ttl=180)
def get_entries_of_upload(
    upload_id: str,
    url: str = None,
    with_authentication: bool = False,
    timeout_in_sec: int = 10,
) -> list[NomadEntry]:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info(f'retrieving entries for upload {upload_id} on {url_name} server')
    response = get_nomad_request(
        f'/uploads/{upload_id}/entries',
        with_authentication=with_authentication,
        url=url,
        timeout_in_sec=timeout_in_sec,
    )
    nomad_entry_schema = class_schema(NomadEntry, base_schema=NomadEntrySchema)
    return [
        nomad_entry_schema().load({**r['entry_metadata'], 'url': url})
        for r in response['data']
    ]


def get_entries_of_my_uploads(
    url: str = None, timeout_in_sec: int = 10
) -> list[NomadEntry]:
    return [
        upload_entry
        for u in get_all_my_uploads(url=url, timeout_in_sec=timeout_in_sec)
        for upload_entry in get_entries_of_upload(
            u.upload_id, with_authentication=True, url=url
        )
    ]


# def get_entries_in_database(database_id: str = DEFAULT_DATABASE, use_prod: bool = DEFAULT_USE_PROD) -> list[NomadEntry]:
#     return query_entries(dataset_id=database_id, use_prod=use_prod)


@ttl_cache(maxsize=128, ttl=180)
def query_entries(
    worfklow_name: str = None,
    program_name: str = None,
    dataset_id: str = None,
    origin: str = None,
    page_size: int = 10,
    max_entries: int = 50,
    url: str = None,
) -> list[NomadEntry]:
    json_dict = {
        'query': {},
        'pagination': {'page_size': page_size},
        'required': {'include': ['entry_id']},
    }
    entries = []
    while (max_entries > 0 and len(entries) <= max_entries) or (max_entries < 0):
        if dataset_id:
            json_dict['query']['datasets'] = {'dataset_id': dataset_id}
        if worfklow_name:
            json_dict['query']['results.method'] = {'workflow_name': worfklow_name}
        if program_name:
            json_dict['query']['results.method'] = {
                'simulation': {'program_name': program_name}
            }
        if origin:
            json_dict['query']['origin'] = origin
        query = post_nomad_request('/entries/query', json_dict=json_dict, url=url)
        entries.extend([q['entry_id'] for q in query['data']])
        next_page_after_value = query['pagination'].get('next_page_after_value', None)
        if next_page_after_value:
            json_dict['pagination']['page_after_value'] = next_page_after_value
        else:
            break
    if max_entries > 0:
        entries = entries[:max_entries]
    return [get_entry_by_id(e, url=url) for e in entries]


def download_entry_raw_data_by_id(
    entry_id: str,
    url: str = None,
    timeout_in_sec: int = 10,
    with_authentication: bool = False,
    zip_file_name: str = None,
) -> ByteString:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('retrieving raw data of entry ID %s on %s server', entry_id, url_name)
    response = get_nomad_request(
        f'/entries/{entry_id}/raw?compress=true',
        with_authentication=with_authentication,
        url=url,
        timeout_in_sec=timeout_in_sec,
        return_json=False,
        accept_field='application/zip',
    )

    if zip_file_name is not None:
        with open(zip_file_name, 'wb') as f:
            f.write(bytes(response))

    return response


def download_entry_by_id(
    entry_id: str,
    url: str = None,
    timeout_in_sec: int = 10,
    with_authentication: bool = False,
    zip_file_name: str = None,
) -> dict:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)

    logger.info('retrieving data of entry ID %s on %s server', entry_id, url_name)
    response = get_nomad_request(
        f'/entries/{entry_id}/archive/download',
        with_authentication=with_authentication,
        url=url,
        timeout_in_sec=timeout_in_sec,
    )

    if zip_file_name is not None:
        __ = download_entry_raw_data_by_id(
            entry_id=entry_id,
            url=url,
            timeout_in_sec=timeout_in_sec,
            with_authentication=with_authentication,
            zip_file_name=zip_file_name,
        )

    return response
