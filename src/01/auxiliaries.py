from commons.utils import Utils
from enum import Enum


class Field(Enum):
    ID = 0
    NAME = 1
    CITY = 2
    COUNTRY = 3
    IATA_CODE = 4
    ICAO_CODE = 5
    LATITUDE = 6
    LONGITUDE = 7
    ALTITUDE = 8
    TIMEZONE_DST = 9
    TIMEZONE_OLSON = 10
