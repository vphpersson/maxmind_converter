#!/usr/bin/env python

from argparse import ArgumentParser
from asyncio import run as asyncio_run
from sys import exit as sys_exit

from httpx import AsyncClient

from maxmind_converter.download import download, RetrievalData


class MaxmindDownloaderArgumentParser(ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **(
                dict(
                    description='Download a Maxmind database.'
                ) | kwargs
            )
        )

        self.add_argument(
            'database',
            choices=['asn', 'country'],
            help='The type of database to retrieve and convert.'
        )

        self.add_argument(
            '--licence-key',
            required=True,
            help='The licence key to use when retrieving the Maxmind database.'
        )

        self.add_argument(
            '-o', '--out-file',
            dest='out_file',
            help='A path of a filo to which to write the database.'
        )

        self.add_argument(
            '--last-file-name',
            help='The name of the last database file that was downloaded, to decide whether a new download is necessary.'
        )


async def main():
    args = MaxmindDownloaderArgumentParser().parse_args()

    edition_id: str
    match args.database:
        case 'asn':
            edition_id = 'GeoLite2-ASN-CSV'
        case 'country':
            edition_id = 'GeoLite2-Country-CSV'
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
        retrieval_data: RetrievalData | None = await download(
            http_client=http_client,
            last_file_name=args.last_file_name
        )

        if not retrieval_data:
            print(args.last_file_name)
            sys_exit(10)
        else:
            print(retrieval_data.file_name)
            with open(file=args.out_file or retrieval_data.file_name, mode='wb') as fp:
                fp.write(retrieval_data.content)

if __name__ == '__main__':
    asyncio_run(main())
