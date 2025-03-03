import datetime as dt
import logging
from dataclasses import asdict, field
from typing import Optional

from cachetools.func import ttl_cache
from marshmallow import EXCLUDE
from marshmallow_dataclass import class_schema, dataclass

from nomad_utility_workflows.utils.core import (
    RequestOptions,
    get_nomad_request,
    get_nomad_url,
    get_nomad_url_name,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NomadUser:
    class Meta:
        unknown = EXCLUDE

    user_id: str = field(repr=False)
    name: str
    first_name: str = field(repr=False)
    last_name: str = field(repr=False)
    username: str = field(repr=False)
    affiliation: Optional[str] = field(repr=False, default=None)
    affiliation_address: Optional[str] = field(repr=False, default=None)
    email: Optional[str] = field(repr=False, default=None)
    is_oasis_admin: Optional[bool] = field(repr=False, default=None)
    is_admin: Optional[bool] = field(repr=False, default=None)
    repo_user_id: Optional[str] = field(repr=False, default=None)
    created: Optional[dt.datetime] = field(repr=False, default=None)

    def as_dict(self) -> dict:
        return asdict(self)


@ttl_cache(maxsize=128, ttl=180)
def search_users_by_name(
    user_name: str, url: str = None, timeout_in_sec: int = 10
) -> NomadUser:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('retrieving user %s on %s server', user_name, url_name)
    response = get_nomad_request(
        RequestOptions(
            section=f'/users?prefix={user_name}', timeout_in_sec=timeout_in_sec, url=url
        )
    ).get('data', [])
    return [class_schema(NomadUser)().load(user) for user in response]


@ttl_cache(maxsize=128, ttl=180)
def get_user_by_id(
    user_id: str, url: str = None, timeout_in_sec: int = 10
) -> NomadUser:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('retrieving user %s on %s server', user_id, url_name)
    response = get_nomad_request(
        RequestOptions(
            section=f'/users/{user_id}', timeout_in_sec=timeout_in_sec, url=url
        )
    )
    user_schema = class_schema(NomadUser)
    return user_schema().load(response)


@ttl_cache(maxsize=128, ttl=180)
def who_am_i(url: str = None, timeout_in_sec: int = 10) -> NomadUser:
    url = get_nomad_url(url)
    url_name = get_nomad_url_name(url)
    logger.info('retrieving self user info on %s server', url_name)
    response = get_nomad_request(
        RequestOptions(
            section='/users/me',
            with_authentication=True,
            timeout_in_sec=timeout_in_sec,
            url=url,
        )
    )
    user_schema = class_schema(NomadUser)
    return user_schema().load(response)
