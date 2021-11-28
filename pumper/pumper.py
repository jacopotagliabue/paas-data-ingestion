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
from random import randint
import os
from datetime import datetime
from events import create_pageview, create_add, create_detail, create_purchase, create_remove
import csv


def post_payload(collect_url: str, event: dict):
    import requests
    # use requests to post the payload
    r = requests.post(collect_url, json=event)
    # just return the status code as we don't expect anything from 
    # the /collect endpoint if not for a simple acknowledgment
    return r.status_code


def create_artificial_event(row: dict, product_metadata: dict) -> dict:
    if row['event_type'] == 'pageview':
        return create_pageview(row)
    elif row['event_type'] == 'event_product':
        if row['product_action'] == 'detail':
            create_detail(row, product_metadata)
        elif row['product_action'] == 'add':
            create_add(row, product_metadata)
        elif row['product_action'] == 'remove':
            create_remove(row, product_metadata)
        elif row['product_action'] == 'purchase':
            create_purchase(row, product_metadata)
        else:
            raise Exception("Product action not valid: {}".format(row['product_action']))
    else:
        raise Exception("Event type not valid: {}".format(row['event_type']))

    return None


def send_event(row: dict, collect_url: str, product_metadata: dict=None) -> bool:
    """
    Given the metadata in the row of the original dataset, 
    create a GA event and make the HTTP request
    """
    try:
        event = create_artificial_event(row, product_metadata)
        if event:
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


def get_catalog_map(catalog_file: str, is_debug: bool=False) -> dict:
    catalog_map = dict()

    with open(catalog_file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            catalog_map[row['product_sku_hash']] = row

    if is_debug:
        print("We have a total of {} skus".format(len(catalog_map)))

    return catalog_map


def pumper_loop(dataset_file: str, catalog_map: dict, collect_url: str, n_events: int) -> bool:
    """
    
        Loop over the browsing dataset, join with catalog data and send the event to collect

    """
    with open(dataset_file) as csvfile:
        reader = csv.DictReader(csvfile)
        for idx, row in enumerate(reader):
            # quit if limit has been reached
            if idx >= n_events:
                print("Reached {} events: quitting!".format(idx))
                return True
            # print some debugging stuff
            if idx and idx % 200 == 0:
                print("Reached {} events!".format(idx))
            # send event
            is_sent = send_event(
                row,
                collect_url,
                catalog_map.get(row['product_sku_hash'], None) if row['product_sku_hash'] else None
            )
            
    return True

def pump_events(
    data_folder: str,
    collect_url: str,
    n_events: int
):
    """
        Main orchestrating function:

        loop over a dataset of real events, simulate Google Analytics-like payloads and send 
        them to the ingestion endpoint
    """
    print("\n============> Start processing at {}\n".format(datetime.utcnow()))
    # first, get a map product id (SKU) -> metadata
    catalog_map = get_catalog_map(os.path.join(data_folder, 'sku_to_content.csv'), is_debug=True)
    # loop over browsing event and sends them
    pumper_loop(os.path.join(data_folder, 'browsing_train.csv'), catalog_map, collect_url, n_events)
    # all done, say goodbye
    print("\n============> All done at {}\n\nSee you, space cowboy\n".format(datetime.utcnow()))
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
    assert os.environ['COLLECT_URL']
    # some global vars for the data pump
    N_EVENTS = 100000

    pump_events(
        data_folder=os.environ['DATA_FOLDER'],
        collect_url=os.environ['COLLECT_URL'],
        n_events=N_EVENTS
    )