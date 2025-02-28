import datetime as dt
import logging
from typing import Any, Optional, TypedDict

from cachetools.func import ttl_cache
from marshmallow import EXCLUDE, Schema, pre_load
from marshmallow_dataclass import class_schema, dataclass

from nomad_utility_workflows.utils.core import (
    RequestOptions,
    delete_nomad_request,
    get_nomad_base_url,
    get_nomad_request,
    get_nomad_url,
    get_nomad_url_name,
    post_nomad_request,
)
from nomad_utility_workflows.utils.users import NomadUser, get_user_by_id

logger = logging.getLogger(__name__)


class NomadUploadSchema(Schema):
    @pre_load
    def convert_users(self, data, **kwargs):
        data['main_author'] = get_user_by_id(user_id=data['main_author']).as_dict()
        data['writers'] = [get_user_by_id(user_id=w).as_dict() for w in data['writers']]
        data['reviewers'] = [
            get_user_by_id(user_id=r).as_dict() for r in data['reviewers']
        ]
        data['viewers'] = [get_user_by_id(user_id=v).as_dict() for v in data['viewers']]
        return data


class UploadMetadata(TypedDict, total=False):
    class Meta:
        unknown = EXCLUDE

    upload_name: str
    references: list[str]
    dataset_id: str
    embargo_length: float
    coauthors_ids: list[str]
    comment: str


@dataclass(frozen=True)
class NomadUpload:
    class Meta:
        unknown = EXCLUDE

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
    n_entries: Optional[int] = None
    upload_files_server_path: Optional[str] = None
    publish_time: Optional[dt.datetime] = None
    references: Optional[list[str]] = None
    datasets: Optional[list[str]] = None
    external_db: Optional[str] = None
    upload_name: Optional[str] = None
    comment: Optional[str] = None
    url: Optional[str] = None
    complete_time: Optional[dt.datetime] = None

    @property
    def base_url(self) -> Optional[str]:
        url = get_nomad_url(self.url)
        return get_nomad_base_url(url)

    @property
    def nomad_gui_url(self) -> str:
        if self.upload_id is None:
            raise ValueError(f"missing attributes 'upload_id' for entry {self}")
        return f'{self.base_url}/gui/user/uploads/upload/id/{self.upload_id}'


@ttl_cache(maxsize=128, ttl=180)
def get_all_my_uploads(url: str = None, timeout_in_sec: int = 10) -> list[NomadUpload]:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('retrieving all uploads on %s server', url_name)
    response = get_nomad_request(
        RequestOptions(
            section='/uploads',
            url=url,
            with_authentication=True,
            timeout_in_sec=timeout_in_sec,
        )
    )
    upload_class_schema = class_schema(NomadUpload, base_schema=NomadUploadSchema)
    return [upload_class_schema().load({**r, 'url': url}) for r in response['data']]


def get_upload_by_id(
    upload_id: str, url: str = None, timeout_in_sec: int = 10
) -> NomadUpload:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('retrieving upload %s on %s server', upload_id, url_name)
    response = get_nomad_request(
        RequestOptions(
            section=f'/uploads/{upload_id}',
            url=url,
            with_authentication=True,
            timeout_in_sec=timeout_in_sec,
        )
    )
    upload_class_schema = class_schema(NomadUpload, base_schema=NomadUploadSchema)
    return upload_class_schema().load({**response['data'], 'url': url})


def delete_upload(
    upload_id: str, url: str = None, timeout_in_sec: int = 10
) -> NomadUpload:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info(f'deleting upload {upload_id} on {url_name} server')
    response = delete_nomad_request(
        RequestOptions(
            section=f'/uploads/{upload_id}',
            with_authentication=True,
            url=url,
            timeout_in_sec=timeout_in_sec,
        )
    )
    upload_class_schema = class_schema(NomadUpload, base_schema=NomadUploadSchema)
    return upload_class_schema().load({**response['data'], 'url': url})


def upload_files_to_nomad(
    filename: str, url: str = None, timeout_in_sec: int = 30
) -> str:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('uploading file %s on %s server', filename, url_name)
    with open(filename, 'rb') as f:
        response = post_nomad_request(
            RequestOptions(
                section='/uploads',
                with_authentication=True,
                url=url,
                timeout_in_sec=timeout_in_sec,
            ),
            data=f,
        )
    upload_id = response.get('upload_id')
    if upload_id:
        logger.info('successful upload to %s', upload_id)
        return upload_id
    else:
        logger.error('could not upload %s. Response %s', filename, response)


def publish_upload(upload_id: str, url: str = None, timeout_in_sec: int = 10) -> dict:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('publishing upload %s on %s server', upload_id, url_name)
    response = post_nomad_request(
        RequestOptions(
            section=f'/uploads/{upload_id}/action/publish',
            with_authentication=True,
            url=url,
            timeout_in_sec=timeout_in_sec,
        )
    )
    return response


def edit_upload_metadata(
    upload_id: str,
    upload_metadata: UploadMetadata = {},
    url: str = None,
    timeout_in_sec: int = 10,
) -> dict:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('editing the metadata for upload %s on %s server', upload_id, url_name)
    metadata = {'metadata': {}}
    if 'dataset_id' in upload_metadata.keys():
        upload_metadata['datasets'] = upload_metadata.pop('dataset_id')
    for key, value in upload_metadata.items():
        metadata['metadata'][key] = value
    response = post_nomad_request(
        RequestOptions(
            section=f'/uploads/{upload_id}/edit',
            url=url,
            with_authentication=True,
            timeout_in_sec=timeout_in_sec,
        ),
        json_dict=metadata,
    )
    return response
