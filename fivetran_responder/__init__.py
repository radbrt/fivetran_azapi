import logging
import requests
import azure.functions as func
import datetime
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    ENDPOINT = 'https://arbeidsplassen.nav.no/public-feed/api/v1/ads'

    job = req.get_json()
    TOKEN = job.get('secrets').get('token')
    HEADERS = {"accept": "application/json", "Authorization": f"Bearer {TOKEN}"}

    prev_invoke = job.get('state').get('cursor') or '2021-12-05T16:43:45'
    prev_invoke = prev_invoke.replace('Z', '')
    current_highwater = job.get('state').get('highwater') or ''

    prev_page = job.get('state').get('page')
    current_page = 0 if prev_page is None else prev_page + 1
    logging.info(prev_invoke)

    endtime = "*"
    args = f"size=100&published=%5B{prev_invoke}%2C{endtime}%5D"
 
    request_url = f"{ENDPOINT}?{args}&page={current_page}"

    ads_page = requests.get(request_url, headers=HEADERS)
    logging.info(request_url)
    logging.info(ads_page.status_code)

    if ads_page.ok:
        ads_json = ads_page.json()
        ads_content = ads_json.get('content')
        published_times = [ad.get('published') for ad in ads_content] or [prev_invoke]
        published_times.append(current_highwater)
        highwater_mark = max(published_times)

        has_more = not ads_json.get('last')

        if has_more:
            return_state = {
                "cursor": prev_invoke,
                "highwater": highwater_mark,
                "page": current_page
            }
        else:
            return_state = {
                "cursor": highwater_mark
            }

        return func.HttpResponse(
            json.dumps({
                "state": return_state,
                "insert": {
                    "nav_job_ads_api": ads_content
                },
                "schema": {
                    "nav_job_ads_api": {
                        "primary_key": ["uuid"]
                    }
                }, 
                "hasMore": has_more
            })
            , status_code=ads_page.status_code,  
            mimetype="application/json",
        )
    else:
        return func.HttpResponse(
             "Some error with the endpoint here, not really cool at all",
             status_code=ads_page.status_code
        )
