from dataclasses import dataclass, asdict
from zipfile import ZipFile
from csv import DictReader
from pathlib import PurePath
from io import TextIOWrapper, BytesIO
from ipaddress import ip_network, IPv4Network, IPv6Network


@dataclass
class CountryRangeEntry:
    network: IPv4Network | IPv6Network
    country_iso_code: str | None = None


@dataclass
class ASNRangeEntry:
    network: IPv4Network | IPv6Network
    as_number: int | None = None
    as_organization: str | None = None


def convert_asn_database(zip_file: ZipFile) -> list[ASNRangeEntry]:
    directory_name: str = PurePath(next(iter(zip_file.filelist)).filename).parent.name

    ipv4_csv_data: bytes = zip_file.read(name=f'{directory_name}/GeoLite2-ASN-Blocks-IPv4.csv')
    ipv6_csv_data: bytes = zip_file.read(name=f'{directory_name}/GeoLite2-ASN-Blocks-IPv6.csv')

    return [
        ASNRangeEntry(
            network=ip_network(address=row['network']),
            as_number=row['autonomous_system_number'],
            as_organization=row['autonomous_system_organization']
        )
        for data in (ipv4_csv_data, ipv6_csv_data)
        for row in DictReader(TextIOWrapper(BytesIO(data), newline=''))
    ]


def convert_country_database(zip_file: ZipFile) -> list[CountryRangeEntry]:
    """

    :param zip_file:
    :return: A list of entries of networks and their corresponding country ISO code.
    """

    directory_name: str = PurePath(next(iter(zip_file.filelist)).filename).parent.name

    ipv4_csv_data: bytes = zip_file.read(name=f'{directory_name}/GeoLite2-Country-Blocks-IPv4.csv')
    ipv6_csv_data: bytes = zip_file.read(name=f'{directory_name}/GeoLite2-Country-Blocks-IPv6.csv')

    country_data: bytes = zip_file.read(name=f'{directory_name}/GeoLite2-Country-Locations-en.csv')

    geoname_id_to_country_iso_code: dict[str, str] = {
        row['geoname_id']: row['country_iso_code']
        for row in DictReader(TextIOWrapper(BytesIO(country_data), newline=''))
    }

    return [
        CountryRangeEntry(
            network=ip_network(address=row['network']),
            country_iso_code=geoname_id_to_country_iso_code.get(row['geoname_id']),
        )
        for data in (ipv4_csv_data, ipv6_csv_data)
        for row in DictReader(TextIOWrapper(BytesIO(data), newline=''))
    ]
