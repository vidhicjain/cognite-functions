
from cognite.client.exceptions import CogniteException, CogniteAPIError
from cognite.client.credentials import OAuthClientCredentials
from cognite.client import CogniteClient, ClientConfig
from handler import handle
import json


TENANT_ID="e0793d39-0939-496d-b129-198edd916feb"
CLIENT_ID="c1f94aa4-bf73-4c93-93d8-d4bc58df0627"
CDF_CLUSTER="api"
COGNITE_PROJECT="accenture-tiger-training"
CLIENT_SECRET = "b2kwbWhqI09ZeiFVeURzWQ=="

BASE_URL = f"https://{CDF_CLUSTER}.cognitedata.com"
SCOPES = [f"https://{CDF_CLUSTER}.cognitedata.com/.default"]
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
AUTHORITY_HOST_URI = "https://login.microsoftonline.com"
AUTHORITY_URI = AUTHORITY_HOST_URI + "/" + TENANT_ID
PORT = 53000

creds=OAuthClientCredentials(
    token_url=TOKEN_URL, 
    client_id= CLIENT_ID, 
    scopes= SCOPES, 
    client_secret= CLIENT_SECRET
    )
cnf = ClientConfig(client_name="my-special-client", project=COGNITE_PROJECT, credentials=creds)
client = CogniteClient(cnf)
client.login.status()
# c = cauth.client_secret_client()


def deploy_backfill_events_function(data: dict = {}):

    try:
        # create Cognite function to execute the task to backfill events. Function after deploying is not working

        # func = c.functions.create(
        #     name="3w-back-fill-function",
        #     external_id="3w-back-fill-function",
        #     folder='initial_data_load/'

        # )
        result = handle(data,client)
        # print(f'Function returned {result}')

        # print(f'Hi')
        # call the function
        # call = func.call(data=data)
        # call the function locally
        #result = handler.handle(c, data)
        #print(f'Function returned from local  {result}')

    except (CogniteAPIError, ValueError, CogniteException) as error:
        print(f'Error occured {error}')


with open("petro_function2/timeseries.json", "r") as jsonfile:
    data = json.load(jsonfile)

deploy_backfill_events_function(data)
