from nifi_airflow_connector import get_token, get_processor, update_processor_status, pause

def getFile():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "3f2679e2-18a3-3caa-b7f5-887da076ca2f"
    access_payload = {"username" : "heritiana","password" : "Haikosyboulie2"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)
 

def routeOnAttribute():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "418260fc-0185-1000-cd69-f55964da16b6"
    access_payload = {"username" : "heritiana","password" : "Haikosyboulie2"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)

def putFile1():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "01851003-35f8-10cb-0a07-7985cc95a58b"
    access_payload = {"username" : "heritiana","password" : "Haikosyboulie2"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)

def updateAttributeTour1():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "01851000-3840-10c5-bb55-9a64278edab6"
    access_payload = {"username" : "heritiana","password" : "Haikosyboulie2"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)

def updateAttributeTour2():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "01851005-3840-10c5-9d15-bee8284c4dbe"
    access_payload = {"username" : "heritiana","password" : "Haikosyboulie2"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)

def publishKafkaRecord_2_6Tour1():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "41b01e1b-0185-1000-e352-acb4d0923bbd"
    access_payload = {"username" : "heritiana","password" : "Haikosyboulie2"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)

def publishKafkaRecord_2_6Tour2():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "4ee56a11-0185-1000-1ad0-9f4126b10d6e"
    access_payload = {"username" : "heritiana","password" : "Haikosyboulie2"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)

def putFileTour1():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "01851000-35f8-10cb-13aa-6c56e82c8ae7"
    access_payload = {"username" : "heritiana","password" : "Haikosyboulie2"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)
    pause(120)

def putFileTour2():
    url_nifi_api ="https://127.0.0.1:8443/nifi-api/"
    processor_id = "01851004-3840-10c5-4fd3-06f5d84b3a12"
    access_payload = {"username" : "heritiana","password" : "Haikosyboulie2"}

    token = get_token(url_nifi_api, access_payload)
    processor = get_processor(url_nifi_api, processor_id, token)
    response = update_processor_status(processor_id,"RUNNING",token,url_nifi_api)
    pause(120)
