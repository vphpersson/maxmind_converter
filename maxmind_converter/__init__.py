from dataclasses import dataclass, asdict
from zipfile import ZipFile
from csv import DictReader
from pathlib import PurePath
from io import TextIOWrapper, BytesIO
from ipaddress import ip_network, IPv4Network, IPv6Network


@dataclass
class RangeEntry:
    network: IPv4Network | IPv6Network
    country_iso_code: str | None = None
    as_number: int | None = None
    as_organization: str | None = None


@dataclass
class _ASNInfo:
    as_number: int
    as_organization: str


def convert_database(
    country_database_zip_file: ZipFile,
    asn_database_zip_file: ZipFile | None
) -> list[RangeEntry]:
    """

    :param country_database_zip_file
    :param asn_database_zip_file
    :return: A list of entries of networks and their corresponding country ISO code.
    """

    country_directory_name: str = PurePath(next(iter(country_database_zip_file.filelist)).filename).parent.name

    county_ipv4_csv_data: bytes = country_database_zip_file.read(
        name=f'{country_directory_name}/GeoLite2-Country-Blocks-IPv4.csv'
    )
    county_ipv6_csv_data: bytes = country_database_zip_file.read(
        name=f'{country_directory_name}/GeoLite2-Country-Blocks-IPv6.csv'
    )

    country_data: bytes = country_database_zip_file.read(
        name=f'{country_directory_name}/GeoLite2-Country-Locations-en.csv'
    )

    geoname_id_to_country_iso_code: dict[str, str] = {
        row['geoname_id']: row['country_iso_code']
        for row in DictReader(TextIOWrapper(BytesIO(country_data), newline=''))
    }

    network_to_asn_info: dict[str, _ASNInfo] = {}
    if asn_database_zip_file:
        asn_directory_name: str = PurePath(next(iter(asn_database_zip_file.filelist)).filename).parent.name

        asn_ipv4_csv_data: bytes = asn_database_zip_file.read(
            name=f'{asn_directory_name}/GeoLite2-ASN-Blocks-IPv4.csv'
        )
        for row in DictReader(TextIOWrapper(BytesIO(asn_ipv4_csv_data), newline='')):
            network_to_asn_info[row['network']] = _ASNInfo(
                as_number=row['autonomous_system_number'],
                as_organization=row['autonomous_system_organization']
            )

        asn_ipv6_csv_data: bytes = asn_database_zip_file.read(
            name=f'{asn_directory_name}/GeoLite2-ASN-Blocks-IPv6.csv'
        )
        for row in DictReader(TextIOWrapper(BytesIO(asn_ipv6_csv_data), newline='')):
            network_to_asn_info[row['network']] = _ASNInfo(
                as_number=row['autonomous_system_number'],
                as_organization=row['autonomous_system_organization']
            )

    return [
        RangeEntry(
            network=ip_network(address=row['network']),
            country_iso_code=geoname_id_to_country_iso_code.get(row['geoname_id']),
            **(asdict(as_info) if (as_info := network_to_asn_info.get(row['network'])) else {})
        )
        for data in (county_ipv4_csv_data, county_ipv6_csv_data)
        for row in DictReader(TextIOWrapper(BytesIO(data), newline=''))
    ]
