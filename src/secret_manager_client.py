import signal
import threading
import json
import datetime
import dateutil.tz
import requests
import boto3
import urllib3
import websocket

from typing import Optional, Callable

eastern_time_zone = dateutil.tz.gettz('US/Eastern')
urllib3.disable_warnings()

class SecretManagerClient:
    # TODO: Either an instance of the client couples 1:1 with the cluster or an instance of the Client couples 1:3 with
    #  the 3 possible clusters (I think I like client per, but then a problem is the user can make multiple clients for
    #  the same cluster and that's not desirable behavior,
    #  somehow keeping track with multiple Client instances will be the difficulty)
    def __init__(self, region: str, process_message: Optional[Callable[[str], None]] = None,
                 on_close: Optional[Callable[[websocket.WebSocketApp], None]] = None,
                 on_error: Optional[Callable[[websocket.WebSocketApp, str], None]] = None):
        self.region = region
        print ("SecretManagerClient - __init__ complete.")

    def getSecretString(self, secret_id):
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.region
        )

        decoded_binary_secret = ''

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_id)
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                decoded_binary_secret = eval(get_secret_value_response['SecretString'])
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        
        return decoded_binary_secret
