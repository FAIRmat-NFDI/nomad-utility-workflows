import datetime as dt
import logging
from typing import Optional, TypedDict

from marshmallow import EXCLUDE, Schema, pre_load
from marshmallow_dataclass import class_schema, dataclass

from nomad_utility_workflows.utils.core import (
    RequestOptions,
    delete_nomad_request,
    get_nomad_request,
    get_nomad_url,
    get_nomad_url_name,
    post_nomad_request,
)
from nomad_utility_workflows.utils.users import NomadUser, get_user_by_id

logger = logging.getLogger(__name__)


class NomadDatasetSchema(Schema):
    @pre_load
    def convert_users(self, data, **kwargs):
        data['user'] = get_user_by_id(user_id=data['user_id']).as_dict()
        del data['user_id']
        return data


class DatasetParams(TypedDict, total=False):
    dataset_id: str
    dataset_name: str
    user_id: str
    page_size: int
    max_datasets: int


default_dataset_params = {
    'dataset_id': None,
    'dataset_name': None,
    'user_id': None,
    'page_size': 10,
    'max_datasets': 50,
}


@dataclass(frozen=True)
class NomadDataset:
    class Meta:
        unknown = EXCLUDE

    dataset_id: str
    dataset_create_time: dt.datetime
    dataset_name: str
    dataset_type: Optional[str] = None
    dataset_modified_time: Optional[dt.datetime] = None
    user: Optional[NomadUser] = None
    doi: Optional[str] = None
    pid: Optional[int] = None
    m_annotations: Optional[dict] = None


def retrieve_datasets(
    dataset_params: DatasetParams = default_dataset_params.copy(),
    url: str = None,
) -> list[NomadDataset]:
    parameters = []
    max_datasets = dataset_params.pop(
        'max_datasets', default_dataset_params['max_datasets']
    )
    for key, value in dataset_params.items():
        parameters.append(f'{key}={value}')
    url = get_nomad_url(url)
    section = '/datasets/'
    if len(parameters) > 0:
        section += f'?{parameters[0]}'
    for i in range(1, len(parameters)):
        section += f'&{parameters[i]}'
    headers = {'Accept': 'application/json'}
    nomad_entry_schema = class_schema(NomadDataset, base_schema=NomadDatasetSchema)
    datasets = []
    page_after_value = None
    while (max_datasets > 0 and len(datasets) <= max_datasets) or (max_datasets < 0):
        section = (
            f'{section}&page_after_value={page_after_value}'
            if page_after_value
            else section
        )
        response = get_nomad_request(
            RequestOptions(section=section, headers=headers, url=url)
        )
        if len(response['data']) == 0:
            break
        datasets.extend([nomad_entry_schema().load(d) for d in response['data']])
        if response['pagination']['page'] == response['pagination']['total']:
            break
        page_after_value = response['pagination'].get('next_page_after_value')
    return datasets


def get_dataset_by_id(dataset_id: str, url: str = None) -> NomadDataset:
    datasets = retrieve_datasets(DatasetParams(dataset_id=dataset_id), url=url)
    if len(datasets) != 1:
        raise ValueError(f'Problem retrieving dataset {dataset_id}: {datasets}')
    return datasets[0]


def create_dataset(dataset_name: str, url: str = None, timeout_in_sec: int = 10) -> str:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('creating dataset name %s on %s server', dataset_name, url_name)
    json_dict = {'dataset_name': dataset_name}
    response = post_nomad_request(
        RequestOptions(
            section='/datasets/',
            with_authentication=True,
            url=url,
            timeout_in_sec=timeout_in_sec,
        ),
        json_dict=json_dict,
    )
    return response.get('dataset_id')


def delete_dataset(dataset_id: str, url: str = None, timeout_in_sec: int = 10) -> None:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('deleting dataset %s on %s server', dataset_id, url_name)
    response = delete_nomad_request(
        RequestOptions(
            section=f'/datasets/{dataset_id}',
            with_authentication=True,
            url=url,
            timeout_in_sec=timeout_in_sec,
        )
    )
    if response.get('dataset_id'):
        logger.info('successfully deleted dataset %s', dataset_id)
    else:
        logger.error('no dataset deleted')
