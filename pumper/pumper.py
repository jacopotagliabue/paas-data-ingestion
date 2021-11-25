"""

    This simple script will use real-world e-commerce data coming from the Coveo dataset to send realistic 
    events to the /collect endpoint that powers the ingestion pipeline at the heart of this repository.

    The dataset is freely available for research purposes at:

    https://github.com/coveooss/SIGIR-ecom-data-challenge

    Don't forget to star the repo if you use the dataset ;-)

    Of course, you could send to /collect your own Google Analytics-like events if you wish, or even totally 
    random events: while nothing hinges on the specificity of the chosen data stream, we thought it was better for
    pedagogical reasons to actually use events coming from a real distribution.

"""


import time
from random import choice, randint
import uuid
import os
import requests


def post_payload(collect_url: str, event: dict):
    r = requests.post(collect_url, json=event)
    # just return the status code as we don't expect anything from 
    # the /collect endpoint if not for a simple acknowledgment
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


def send_event(
    row: dict,
    collect_url: str,
    ):
    """
    Given the metadata in the row of the original dataset, create a GA event 
    and make the HTTP request
    """
    try:
        event = create_artificial_event()
        status_code = post_payload(collect_url, event)
    except Exception as ex:
        # we don't really care about errors here and there, 
        # as long as most messages are sent over - we just print errors for inspections
        print("ERROR IN POSTING TO COLLECT: \n {} \n".format(ex))
        return False
    finally:
        # randomize sending intervals
        time.sleep(randint(5, 500) / 1000.0)

    return True


def pump_events(
    data_folder: str,
    collect_url: str,
    n_events: int

):
    """
        Loop over a dataset of real events, simulate Google Analytics-like payloads and send 
        them to the ingestion endpoint
    """





    return


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except:
        pass

    # make sure we have the data folder variable set, otherwise fail
    # this should be the folder containing the csv file from the Coveo Dataset
    assert os.environ['DATA_FOLDER']
    # make sure we know where to send events
    assert os.environ['COLLECT']
    # some global vars for the data pump
    N_EVENTS = 100000

    pump_events(
        data_folder=os.environ['DATA_FOLDER'],
        collect_url=os.environ['COLLECT'],
        n_events=N_EVENTS
    )