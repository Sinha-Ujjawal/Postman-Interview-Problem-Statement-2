from random import random
from typing import Dict, Iterable, List, Optional
from dataclasses import dataclass, field
import requests
import time
from jsonschema import validate
import logging

BASE_URL = "https://public-apis-api.herokuapp.com/api/v1"

logger = logging.getLogger("api")
logger.setLevel(logging.DEBUG)
# create file handler that logs debug and higher level messages
fh = logging.FileHandler("api.log", mode="w")
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


@dataclass
class Api:
    token: Optional[str] = field(default=None, repr=True)
    max_attempts: int = field(default=10)


def make_headers(api: Api) -> Dict[str, str]:
    ret = {}
    if api.token:
        ret.update({"Authorization": f"Bearer {api.token}"})
    return ret


TOKEN_SCHEMA = {"properties": {"token": {"type": "string"}}}


def set_auth_token(api: Api) -> None:
    logger.debug("Reauthentication")
    url = f"{BASE_URL}/auth/token"
    api.token = None
    while True:  # TODO: Could be very dangerous!
        resp = get(url, api)
        if resp:
            resp_json = resp.json()
            validate(resp_json, TOKEN_SCHEMA)
            api.token = resp_json["token"]
            break


def get(url: str, api: Api) -> Optional[requests.Response]:
    logger.debug(f"Get Request: {url}")
    attempts = 0
    t = 1
    while attempts < api.max_attempts:
        headers = make_headers(api)
        response = requests.get(url, headers=headers)

        if response.status_code == 429:  # max request made
            logger.debug(
                f"Max Request Made! Total attempts: {attempts} made out of {api.max_attempts}"
            )
            tts = t + random()
            time.sleep(tts)
            attempts += 1
            t <<= 1
        elif response.status_code != 200:
            set_auth_token(api)
        else:
            return response
    return None


def get_paged_response(base_url: str, api: Api) -> Iterable[requests.Response]:
    page = 1
    while True:  # could be very dangerous
        url = get_prepared_url(base_url, page=page)
        payload = get(url, api)
        if not payload:
            break
        yield payload
        logger.debug(f"Page: {page} of url: {base_url} read")
        page += 1


Categories = List[str]
CATEGORIES_SCHEMA = {
    "type": "object",
    "properties": {
        "count": {"type": "integer"},
        "categories": {"type": "array", "items": {"type": "string"}},
    },
}


def get_categories(api: Api) -> Iterable[Categories]:
    for res in get_paged_response(f"{BASE_URL}/apis/categories", api):
        res_as_json = res.json()
        validate(res_as_json, CATEGORIES_SCHEMA)
        categories = res_as_json["categories"]
        if not categories:
            break
        yield categories


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
    for res in get_paged_response(
        get_prepared_url(f"{BASE_URL}/apis/entry", category=category), api
    ):
        res_as_json = res.json()
        validate(res_as_json, CATEGORY_APIS_SCHEMA)
        if not res_as_json["categories"]:  # empty categories
            break
        for data in res_as_json["categories"]:
            link = data["Link"]
            yield CategoryApi(category=category, api=link)


def get_catgory_apis(max_attempts: int = 10) -> Iterable[CategoryApi]:
    api: Api = Api(max_attempts=max_attempts)
    for categories in get_categories(api):
        for category in categories:
            yield from get_category_apis_from_category(category, api)
