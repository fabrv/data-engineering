if 'sensor' not in globals():                       
    from mage_ai.data_preparation.decorators import sensor

import requests
from datetime import datetime
import logging


BASE_URL    = 'https://s3.amazonaws.com/tripdata/'
YEARS       = list(range(2013, 2024))               # 2013-2023 inclusive
TIMEOUT_SEC = 10

logger = logging.getLogger(__name__)


def zip_exists(year: int) -> bool:
    url = f'{BASE_URL}{year}-citibike-tripdata.zip'
    try:
        ok = requests.head(url, timeout=TIMEOUT_SEC).status_code == 200
        logger.info(f'HEAD {url} → {"200 OK" if ok else "404"}')
        return ok
    except requests.RequestException as err:
        logger.warning(f'HEAD {url} failed: {err}')
        return False


@sensor
def wait_for_data(**kwargs) -> bool:
    return all(zip_exists(y) for y in YEARS)
