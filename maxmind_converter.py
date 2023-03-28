#!/usr/bin/env python

from argparse import ArgumentParser
from typing import Any
from ipaddress import IPv4Network, IPv6Network
from asyncio import run as asyncio_run
from io import BytesIO
from zipfile import ZipFile
from json import dumps as json_dumps
from dataclasses import asdict

from httpx import AsyncClient

from maxmind_converter.download import download, RetrievalData
from maxmind_converter import convert_database, RangeEntry


class MaxmindConverterArgumentParser(ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **(
                dict(
                    description='Convert Maxmind databases to JSON.'
                ) | kwargs
            )
        )

        self.add_argument('--licence-key', required=True)


def json_dumps_default(obj: Any):
    if isinstance(obj, IPv4Network):
        return str(obj)
    elif isinstance(obj, IPv6Network):
        return str(obj)

    raise TypeError(f'Unexpected dumps type: {type(obj)}')


async def main():
    args = MaxmindConverterArgumentParser().parse_args()

    http_client_options = dict(
        params=dict(
            license_key=args.licence_key,
            suffix='zip'
        )
    )
    http_client: AsyncClient
    async with AsyncClient(**http_client_options) as http_client:
        http_client.params = http_client.params.set('edition_id', 'GeoLite2-Country-CSV')
        country_retrieval_data: RetrievalData = await download(http_client=http_client)

        http_client.params = http_client.params.set('edition_id', 'GeoLite2-ASN-CSV')
        asn_retrieval_data: RetrievalData = await download(http_client=http_client)

    with (
        ZipFile(file=BytesIO(initial_bytes=country_retrieval_data.content), mode='r') as county_zip_file,
        ZipFile(file=BytesIO(initial_bytes=asn_retrieval_data.content), mode='r') as asn_zip_file
    ):
        range_entries: list[RangeEntry] = convert_database(
            country_database_zip_file=county_zip_file,
            asn_database_zip_file=asn_zip_file
        )

    print(
        json_dumps(
            list(map(asdict, range_entries)),
            default=json_dumps_default
        )
    )


if __name__ == '__main__':
    asyncio_run(main())
