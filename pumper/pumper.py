import requests
import time
from random import choice
import uuid
import os


IS_DEBUG = True  # if true, print more stuff in CloudWatch for debugging purposes 
# need to save in the ENV variables the URL corresponding to the AWS lambda
# deployed as the collect endpoint
# save it as: https://XXXX.execute-api.us-west-2.amazonaws.com/dev for example, with no / 
# at the end
# TODO: FIX THIS AS NOW IT IS HARDCODED IN YML, BUT WE SHOULD PASS THIS FROM CLI WHEN DEPLOYING
COLLECT_URL = '{}/collect'.format(os.environ['COLLECT'])
print('Target URL is {}'.format(COLLECT_URL))
N_EVENTS_PER_RUN = 50
# create X random "clients" to assign events to
CIDs = [str(uuid.uuid4()) for _ in range(5)] 


def post_payload(collect_url: str, event: dict):
    r = requests.post(collect_url, json=event)
    return r.status_code


def create_artificial_event():
    client_event = { 
        "t": "pageview",
        "dl": "http://www.jacopotagliabue.it/",
        "tm": round(time.time() * 1000), # current epoch in millisec
        "z": str(uuid.uuid4()),
        "sr": "1440x900",
        "sd": "30-bit",
        "ul": "it-IT",
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36",
        "de": "UTF-8",
        "cid": choice(CIDs) # each time we push an event we pick a random client id to simulate a multi-tenant scenario
        }

    return client_event


def pump(event, context):
    """
    This function runs on a chron and just simulate Google Analytics-like payloads sent to the 
    ingestion endpoint
    """

    # every run, create N events
    for idx in range(0, N_EVENTS_PER_RUN):
        try:
            event = create_artificial_event()
            if IS_DEBUG:
                print(event)
            status_code = post_payload(COLLECT_URL, event)
        except Exception as ex:
            # we don't really care about errors here and there, 
            # as long as most messages are sent over - we just log
            # errors in Cloudwatch for inspections
            print("ERROR IN POSTING TO COLLECT", ex)
        finally:
            time.sleep(0.5)

    return
    

def pump_events():



    return


if __name__ == "__main__":
    pump_events()