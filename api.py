from random import random
from typing import Dict, Iterable, List, Optional, Tuple
from dataclasses import dataclass, field, replace
import requests
import time
from jsonschema import validate
import logging

BASE_URL = "https://public-apis-api.herokuapp.com/api/v1"

logger = logging.getLogger("api")
logger.setLevel(logging.DEBUG)
# create file handler that logs debug and higher level messages
fh = logging.FileHandler("api.log")
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh)


def get_prepared_url(base_url: str, **params):
    req = requests.PreparedRequest()
    req.prepare_url(base_url, params)
    return req.url


@dataclass(frozen=True)
class Api:
    token: Optional[str] = field(default=None, repr=True)
    max_attempts: int = field(default=10)


def make_headers(api: Api) -> Dict[str, str]:
    return {"Authorization": f"Bearer {api.token}"}


TOKEN_SCHEMA = {"properties": {"token": {"type": "string"}}}


def with_auth_token_set(api: Api) -> Api:
    logger.debug("Reauthentication")
    url = f"{BASE_URL}/auth/token"
    resp_json = requests.get(url).json()
    validate(resp_json, TOKEN_SCHEMA)
    return replace(api, token=resp_json["token"])


def get(url: str, api: Api) -> Optional[Tuple[requests.Response, Api]]:
    logger.debug(f"Get Request: {url}")
    attempts = 0
    t = 1
    while attempts < api.max_attempts:
        headers = make_headers(api)
        response = requests.get(url, headers=headers)

        if response.status_code == 429:  # max request made
            tts = t + random()
            time.sleep(tts)
            attempts += 1
            t <<= 1
        elif response.status_code != 200:
            api = with_auth_token_set(api)
        else:
            return (response, api)
    return None


def get_paged_response(
    base_url: str, api: Api
) -> Iterable[Tuple[requests.Response, Api]]:
    page = 1
    while True:  # could be very dangerous
        url = get_prepared_url(base_url, page=page)
        payload = get(url, api)
        if not payload:
            break
        data, api = payload
        if not data:
            break
        yield data, api
        page += 1


Categories = List[str]
CATEGORIES_SCHEMA = {
    "type": "object",
    "properties": {
        "count": {"type": "integer"},
        "categories": {"type": "array", "items": {"type": "string"}},
    },
}


def get_categories(api: Api) -> Iterable[Tuple[Categories, Api]]:
    for res, api in get_paged_response(f"{BASE_URL}/apis/categories", api):
        res_as_json = res.json()
        validate(res_as_json, CATEGORIES_SCHEMA)
        categories = res_as_json["categories"]
        if not categories:
            break
        yield categories, api


@dataclass(frozen=True)
class CategoryApi:
    category: str
    api: str


CATEGORY_APIS_SCHEMA = {
    "properties": {
        "count": {"type": "integer"},
        "categories": {
            "type": "array",
            "items": {
                "properties": {
                    "Link": {"type": "string"},
                },
            },
        },
    }
}


def get_category_apis_from_category(category: str, api: Api) -> Iterable[CategoryApi]:
    for res, api in get_paged_response(
        get_prepared_url(f"{BASE_URL}/apis/entry", category=category), api
    ):
        res_as_json = res.json()
        validate(res_as_json, CATEGORY_APIS_SCHEMA)
        if not res_as_json["categories"]:  # empty categories
            break
        for data in res_as_json["categories"]:
            link = data["Link"]
            yield CategoryApi(category=category, api=link)


def get_catgory_apis() -> Iterable[CategoryApi]:
    api: Api = Api()
    for categories, api in get_categories(api):
        for category in categories:
            yield from get_category_apis_from_category(category, api)
