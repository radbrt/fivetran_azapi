import logging
import requests
import azure.functions as func
import datetime
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    fivetran = req.get_json()

    ACCOUNT = fivetran.get('secrets').get('account')
    TOKEN = fivetran.get('secrets').get('token')
    ENDPOINT = fivetran.get('secrets').get('endpoint')
    HEADERS = {"accept": "application/json", "Authorization": f"Bearer {TOKEN}"}
    LIMIT = 100

    offset = fivetran.get('state').get('offset') or 0
    highwatermark = fivetran.get('state').get('highwatermark') or 0

    request_url = f"{ENDPOINT}/accounts/{ACCOUNT}/runs/?order_by=-id&offset={offset}&limit={LIMIT}"

    r = requests.get(request_url, headers=HEADERS)

    if r.ok:
        runs = r.json()
        run_data = runs.get('data')
        new_data = [run for run in run_data if run['id']>highwatermark]
        if new_data:
            all_run_ids = [run['id'] for run in run_data]
            min_run_id = min(all_run_ids)
            max_run_id = max(all_run_ids)
            reached_watermark = min_run_id <= highwatermark
            fully_paginated = runs.get('extra').get('pagination').get('count') == runs.get('extra').get('pagination').get('total_count')
            has_more = not (reached_watermark or fully_paginated)
            #has_more = not reached_watermark 
        else:
            min_run_id = highwatermark
            max_run_id = highwatermark
            has_more = False


        if has_more:
            return_state = {
                "highwatermark": highwatermark,
                "new_highwatermark": max_run_id,
                "offset": offset + LIMIT
            }
        else:
            return_state = {
                "highwatermark": fivetran.get('state').get('new_highwatermark')  or max_run_id
            }

        return func.HttpResponse(
            json.dumps({
                "state": return_state,
                "insert": {
                    "dbt_runs": new_data
                },
                "schema": {
                    "dbt_runs": {
                        "primary_key": ["id"]
                    }
                }, 
                "hasMore": has_more
            })
            , status_code=runs['status']['code'],  
            mimetype="application/json"
        )

    else:
        return func.HttpResponse(
             json.dumps({
                 'state': fivetran.get('state'),
                'status_code': 500
                }),
             mimetype="application/json"
        )
