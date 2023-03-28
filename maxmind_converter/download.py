from typing import Final, Literal
from re import Pattern as RePattern, compile as re_compile
from dataclasses import dataclass

from httpx import AsyncClient

DOWNLOAD_URL: Final[str] = 'https://download.maxmind.com/app/geoip_download'

_CONTENT_DISPOSITION_PATTERN: Final[RePattern] = re_compile(pattern=r'^attachment; filename=(?P<filename>.+)$')

EditionId = Literal[
    'GeoLite2-ASN',
    'GeoLite2-ASN-CSV',
    'GeoLite2-City',
    'GeoLite2-City-CSV',
    'GeoLite2-Country',
    'GeoLite2-Country-CSV'
]


@dataclass
class RetrievalData:
    content: bytes
    file_name: str


async def _retrieve(
    http_client: AsyncClient,
    method: Literal['HEAD', 'GET'],
    last_file_name: str | None = None
) -> RetrievalData | None:
    """

    :param http_client: An HTTP client with which to perform the HTTP request.
    :param method: The HTTP method of retrieval to use.
    :param last_file_name: The last file name used by the database.
    :return: Data about the retrieval or nothing if the file name is the same as the one provided.
    """

    response = await http_client.request(method=method, url=DOWNLOAD_URL)
    response.raise_for_status()

    if not (match := _CONTENT_DISPOSITION_PATTERN.match(string=response.headers['content-disposition'])):
        raise ValueError('The Content-Disposition value for request did not match the expected format.')
    else:
        file_name = match.groupdict()['filename']

    if file_name == last_file_name:
        return None

    return RetrievalData(content=response.read(), file_name=file_name)


async def download(http_client: AsyncClient, last_file_name: str | None = None) -> RetrievalData | None:
    """

    :param http_client:
    :param last_file_name:
    :return:
    """

    if last_file_name and await _retrieve(http_client=http_client, method='HEAD', last_file_name=last_file_name) is None:
        return None

    return await _retrieve(http_client=http_client, method='GET')
