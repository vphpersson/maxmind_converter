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
from maxmind_converter import convert_asn_database, convert_country_database, ASNRangeEntry, CountryRangeEntry


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

        self.add_argument(
            'database',
            choices=['asn', 'country']
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

    edition_id: str
    match args.database:
        case 'asn':
            edition_id = 'GeoLite2-ASN-CSV'
            convert_func = convert_asn_database
        case 'country':
            edition_id = 'GeoLite2-Country-CSV'
            convert_func = convert_country_database
        case _:
            raise ValueError(f'Unexpected database: {args.database}')

    http_client_options = dict(
        params=dict(
            license_key=args.licence_key,
            suffix='zip',
            edition_id=edition_id
        )
    )
    http_client: AsyncClient
    async with AsyncClient(**http_client_options) as http_client:
        retrieval_data: RetrievalData = await download(http_client=http_client)

    with ZipFile(file=BytesIO(initial_bytes=retrieval_data.content), mode='r') as zip_file:
        print(
            json_dumps(
                list(map(asdict, convert_func(zip_file=zip_file))),
                default=json_dumps_default
            )
        )

if __name__ == '__main__':
    asyncio_run(main())
