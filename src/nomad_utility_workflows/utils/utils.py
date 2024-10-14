import json
import logging
from typing import Any

import requests
from cachetools.func import ttl_cache
from decouple import config as environ

logger = logging.getLogger(__name__)

NOMAD_USERNAME = environ('NOMAD_USERNAME')
NOMAD_PASSWORD = environ('NOMAD_PASSWORD')
NOMAD_PROD_URL = 'https://nomad-lab.eu/prod/v1/api/v1'
NOMAD_STAGING_URL = 'https://nomad-lab.eu/prod/v1/staging/api/v1'
NOMAD_TEST_URL = 'https://nomad-lab.eu/prod/v1/test/api/v1'
STATUS_CODE = 200
TIMEOUT_IN_SEC = 60


@ttl_cache(maxsize=128, ttl=180)
def get_authentication_token(
    url: str = None,
    username: str = NOMAD_USERNAME,
    password: str = NOMAD_PASSWORD,
    timeout_in_sec: int = TIMEOUT_IN_SEC,
) -> str:
    """_summary_

    Args:
        url (str, optional): The NOMAD API URL. Defaults to None.
        username (str, optional): _description_. Defaults to NOMAD_USERNAME.
        password (str, optional): _description_. Defaults to NOMAD_PASSWORD.
        timeout_in_sec (int, optional): _description_. Defaults to TIMEOUT_IN_SEC.

    Raises:
        ValueError: _description_

    Returns:
        str: _description_
    """
    url = get_nomad_url(url)
    logger.info(f'Requesting authentication token @ {url}')
    response = requests.get(
        url + '/auth/token',
        params={'username': username, 'password': password},
        timeout=timeout_in_sec,
    )
    if not response.status_code == STATUS_CODE:
        raise ValueError(f'Unexpected response {response.json()}')
    return response.json().get('access_token')


def get_nomad_request(
    section: str,
    url: str = None,
    timeout_in_sec: int = TIMEOUT_IN_SEC,
    headers: dict = None,
    with_authentication: bool = False,
    return_json: bool = True,
    accept_field: str = 'application/json',
) -> Any:
    """_summary_

    Args:
        section (str): _description_
        url (str, optional): The NOMAD API URL. Defaults to None.
        timeout_in_sec (int, optional): _description_. Defaults to TIMEOUT_IN_SEC.
        headers (dict, optional): _description_. Defaults to None.
        with_authentication (bool, optional): _description_. Defaults to False.
        return_json (bool, optional): _description_. Defaults to True.
        accept_field (str, optional): _description_. Defaults to "application/json".

    Raises:
        ValueError: _description_

    Returns:
        Any: _description_
    """
    url = get_nomad_url(url)
    url += f"{'/' if section[0] != '/' else ''}{section}"
    logger.info(f'Sending get request @ {url}')
    if headers is None:
        headers = {}
    if with_authentication:
        token = get_authentication_token(use_prod=use_prod)
        headers |= {
            'Authorization': f'Bearer {token}',
            'Accept': accept_field,
        }
    response = requests.get(url, headers=headers, timeout=timeout_in_sec)
    if not response.status_code == STATUS_CODE:
        raise ValueError(f'Unexpected response {response.json()}')
    if return_json:
        return response.json()
    return response.content


def get_nomad_url_name(url: str) -> str:
    try:
        return url.split('/')[-3]
    except IndexError:
        return 'unknown'


def get_nomad_url(url: str) -> str:
    """
    Get the Nomad URL based on the given URL. If no URL is given, the NOMAD Test URL
    is returned by default. If the given URL is "prod", "staging" or "test",
    the corresponding Nomad URL will be returned. Otherwise, the given URL will be
    returned.
    """
    if url is None:
        return NOMAD_TEST_URL
    elif url == 'prod':
        return NOMAD_PROD_URL
    elif url == 'staging':
        return NOMAD_STAGING_URL
    elif url == 'test':
        return NOMAD_TEST_URL

    if not url.endswith('/api/v1'):
        logger.warning(
            'The given URL does not appear to be a valid NOMAD API URL, i.e., ending with /api/v1.'
        )

    return url


def get_nomad_base_url(url: str) -> str:
    return (get_nomad_url(url)).removesuffix('/api/v1')


def post_nomad_request(
    section: str,
    headers: dict = None,
    data: Any = None,
    json_dict: dict = None,
    url: str = None,
    timeout_in_sec: int = TIMEOUT_IN_SEC,
    with_authentication: bool = False,
) -> json:
    if headers is None:
        headers = {}
    if with_authentication:
        token = get_authentication_token(url=url)
        headers |= {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
        }
    if data is None:
        data = {}
    if json_dict is None:
        json_dict = {}
    url = get_nomad_url(url)
    url += f"{'/' if section[0] != '/' else ''}{section}"
    logger.info('Sending post request @ %s', url)
    response = requests.post(
        url, headers=headers, json=json_dict, data=data, timeout=timeout_in_sec
    )
    if not response.status_code == STATUS_CODE:
        raise ValueError(f'Unexpected response {response.json()}')
    return response.json()


def delete_nomad_request(
    section: str,
    headers: dict = None,
    url: str = None,
    timeout_in_sec: int = TIMEOUT_IN_SEC,
    with_authentication: bool = False,
) -> json:
    if headers is None:
        headers = {}
    if with_authentication:
        token = get_authentication_token(url=url)
        headers |= {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
        }
    url = get_nomad_url(url)
    url += f"{'/' if section[0] != '/' else ''}{section}"
    logger.info('Sending delete request @ %s', url)
    response = requests.delete(url, headers=headers, timeout=timeout_in_sec)
    if not response.status_code == STATUS_CODE:
        raise ValueError(f'Unexpected response {response.json()}')
    return response.json()
