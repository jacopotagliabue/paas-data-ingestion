import os
import time
import json
import boto3


FIREHOSE_STREAM_NAME = os.environ['fh_stream_name']

fh = boto3.client('firehose')


def handler(event, context):
    res_status_code = 200
    res_headers = {
        'Content-Type': 'image/gif',
        'Access-Control-Allow-Origin': '*',
    }

    body = json.loads(event['body'])
    fh.put_record(
        DeliveryStreamName=FIREHOSE_STREAM_NAME,
        Record={
            'Data': bytes(json.dumps({
                'service': {
                    'id': 'api.collect',
                },
                'context': {
                    'user_ip': body.get('uip', event['multiValueHeaders']['X-Forwarded-For'][0]),
                    'user_agent': body.get('ua', event['headers'].get('User-Agent')),
                    'document_location': body.get('dl'),
                    'document_referrer': body.get('dr'),
                    'client_id': body['cid'],
                    'user_id': body.get('uid'),
                },
                'request': {
                    'id': body['z'],
                    'timestamp': event['requestContext']['requestTimeEpoch'],
                    'hostname': event['requestContext']['domainName'],
                    'path': event['requestContext']['path'],
                    'method': event['requestContext']['httpMethod'],
                    'headers': event['headers'],
                    'body': body,
                },
                'response': {
                    'id': body['z'],
                    'timestamp': int(time.time() * 1000),
                    'statusCode': res_status_code,
                    'headers': res_headers,
                },
            }), 'utf8'),
        }
    )

    return {
        'statusCode': res_status_code,
        'headers': res_headers,
        'body': 'R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7',
        'isBase64Encoded': True,
    }
