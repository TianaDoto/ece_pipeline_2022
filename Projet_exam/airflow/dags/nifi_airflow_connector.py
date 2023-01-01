import requests
import json
from time import time


def pause(secs):
    init_time = time()
    while time() < init_time + secs:
        pass

def get_token(url_nifi_api: str, access_payload: dict):
    """
    Retrieves a JWT token by authenticating the user, makes
    use of the REST API `/access/token`.
    :param url_nifi_api: the basic URL to the NiFi API.
    :param access_payload: dictionary with keys 'username' & 'password' and
                            fitting values.
    :return: JWT Token
    """

    header = {
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "*/*",
    }
    response = requests.post(
        url_nifi_api + "access/token", headers=header, data=access_payload, verify=False
    )
    return response.content.decode("ascii")

def get_processor(url_nifi_api: str, processor_id: str, token=None):
    """
    Gets and returns a single processor.
    Makes use of the REST API `/processors/{processor_id}`.
    :param url_nifi_api: String
    :param processor_id: String
    :param token: JWT access token
    :returns: JSON object processor
    """

    # Authorization header
    header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(token),
    }

    # GET processor and parse to JSON
    response = requests.get(url_nifi_api + f"processors/{processor_id}", headers=header, verify=False)
    return json.loads(response.content)

def update_processor_status(processor_id: str, new_state: str, token, url_nifi_api):
    """Starts or stops a processor by retrieving the processor to get
    the current revision and finally putting a JSON with the desired
    state towards the API.
    :param processor_id: Id of the processor to receive the new state.
    :param new_state: String representing the new state, acceptable
                        values are: STOPPED or RUNNING.
    :param token: a JWT access token for NiFi.
    :param url_nifi_api: URL to the NiFi API
    :return: None
    """

    # Retrieve processor from `/processors/{processor_id}`
    processor = get_processor(url_nifi_api, processor_id, token)

    # Create a JSON with the new state and the processor's revision
    put_dict = {
        "revision": processor["revision"],
        "state": new_state,
        "disconnectedNodeAcknowledged": True,
    }

    # Dump JSON and POST processor
    payload = json.dumps(put_dict).encode("utf8")

    header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(token),
    }

    response = requests.put(
        url_nifi_api + f"processors/{processor_id}/run-status",
        headers=header,
        data=payload,
        verify=False
    )
    #urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return response
    