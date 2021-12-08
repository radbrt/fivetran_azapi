import datetime
import logging
import requests
import azure.functions as func
from requests.api import head
import json

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    # ENDPOINT = 'https://arbeidsplassen.nav.no/public-feed/api/v1/ads'
    # TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwdWJsaWMudG9rZW4udjFAbmF2Lm5vIiwiYXVkIjoiZmVlZC1hcGktdjEiLCJpc3MiOiJuYXYubm8iLCJpYXQiOjE1NTc0NzM0MjJ9.jNGlLUF9HxoHo5JrQNMkweLj_91bgk97ZebLdfx3_UQ'
    # HEADERS = {"accept": "application/json", "Authorization": f"Bearer {TOKEN}"}

    # last_run = datetime.today() - timedelta(hours=1, minutes=15)
    # start_isotime = last_run.isoformat(timespec='seconds')
    # endtime = "*"
    # args = f"size=100&published=%5B{start_isotime}%2C{endtime}%5D"
    # current_page = 0
    # max_page = 100 # placeholder value

    # ads_page = requests.get(f"{ENDPOINT}?{args}?page={current_page}", headers=HEADERS)
    # ads_json = json.loads(ads_page)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
