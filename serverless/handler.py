import json
import uuid
import time


IS_DEBUG = True  # if true, print more stuff in CloudWatch for debugging purposes 
PAYLOAD_VERSION = 'v1' # mark the current version of validation / format


def validate_event(event: dict, version: str):
    """
    Simulate a validation procedure. The procedure returns None if all is valid,
    a dictionary if it is not (containing an error message for downstream processes).
    """
    if version == PAYLOAD_VERSION:
        if 'cid' not in event or not event['cid']:
            return { 
                'errorCode': 422 ,
                'error': 'Parameter missing',
                'errorDescription': '"cid" parameter is missing'
            }

    # if all validation is ok, the error object in the response will be null
    return None


# https://{LAMBDA_URL}/{LAMBDA_ENV}/collect
def collect(event, context):
    """
    Ingest data and return a 1x1 gif pixel
    """
    if IS_DEBUG:
        print(json.dumps(event))
    # get the event in the payload
    client_event = json.loads(event['body'])
    if IS_DEBUG:
        print(client_event)
    # send data to a message broker
    response = drop_message_to_broker(client_event, PAYLOAD_VERSION)
    if IS_DEBUG:
        print(response)
    # finally return the pixel to the client
    return return_pixel_through_gateway()


def drop_message_to_broker(event: dict, version: str):
    """
    Drop event so that it ends up into Snowflake downstream
    """
    # validate the event against some basic syntax rules - e.g. some parameter needs to be there
    is_valid = validate_event(event, version)
    # build a container object wrapping up useful information
    downstream_event = {
        'eventId': str(uuid.uuid4()),  # assign a unique event id to the event
        'rawData': event,
        'version': version,
        'error': is_valid,
        'serverTimestamp': round(time.time() * 1000) # current epoch in millisec
    }

    # drop event to a broker and return a response
    return "Message dropped:\n{}".format(json.dumps(downstream_event))


def return_pixel_through_gateway():
    """
    Abstract the details of the lambda GIF response
    """
    return {
        "statusCode": 200,
        "headers": {
            'Content-Type': 'image/gif'
        },
        "body": "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7",
        "isBase64Encoded": True
    }