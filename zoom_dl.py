#!/usr/bin/env python3
import datetime
import os
import requests
import jwt
from dateutil.parser import parse
import json
from dotenv import load_dotenv

API_ENDPOINT_USER_LIST = 'https://api.zoom.us/v1/user/list'
API2_ENDPOINT_USER_LIST = 'https://api.zoom.us/v2/users'
API_ENDPOINT_RECORDING_LIST = 'https://api.zoom.us/v1/recording/list'
API2_ENDPOINT_RECORDING_LIST1 = 'https://api.zoom.us/v2/users/'
API2_ENDPOINT_RECORDING_LIST2 = '/recordings'

DOWNLOAD_DIRECTORY = 'downloads'
COMPLETED_MEETING_IDS_LOG = 'completed_downloads.txt'
COMPLETED_MEETING_IDS = set()
session_stor = requests.session()


def get_config_vars():
    global API_KEY, API_SECRET
    home_config = os
    load_dotenv()
    try:
        if z_key := os.environ['zoom_api_key']:
            if z_secret := os.environ['zoom_api_secret']:
                API_KEY = z_key
                API_SECRET = z_secret
                return True
    except KeyError:
        #     config not found in .env file or via environmental vars
        pass
    return False


def create_jwt():
    header = {
        'alg': 'HS256',
        "typ": "JWT"
    }
    # set expiration for 4 seconds from now
    expiration = (datetime.datetime.now() + datetime.timedelta(seconds=4)).timestamp()

    payload = {
        'iss': API_KEY,
        'exp': expiration
    }

    s: bytes = jwt.encode(payload=payload, key=API_SECRET, algorithm='HS256')
    return s.decode('utf-8')


def request_post(url, data=None):
    j = create_jwt()
    headers = {
        'authorization': f"Bearer {j}",
        'content-type': "application/json"
    }

    session_stor.headers = headers
    json_data = json.dumps(data)
    response = session_stor.post(url=url, data=json_data)
    return response


def request_get(url, params: dict = None, stream=False, no_jwt=False):
    headers = {}
    if not no_jwt:
        j = create_jwt()
        headers = {
            'authorization': f"Bearer {j}",
            'content-type': "application/json"
        }

    session_stor.headers = headers

    resp = session_stor.get(url=url, params=params, stream=stream)
    return resp


def get_credentials(host_id, page_number):
    return {
        # 'api_key': API_KEY,
        # 'api_secret': API_SECRET,
        'host_id': host_id,
        'page_size': 300,
        'page_number': page_number,
    }


def get_user_ids():
    response = request_get(API2_ENDPOINT_USER_LIST)
    user_data = response.json()
    user_ids = [(user['id'], user['email'],) for user in user_data['users']]
    return user_ids


def format_filename(recording, file_type):
    uuid = recording['uuid']
    topic = recording['topic'].replace('/', '&')
    meeting_time = parse(recording['start_time'])

    return '{} - {} UTC - {}.{}'.format(
        meeting_time.strftime('%Y.%m.%d'), meeting_time.strftime('%I.%M %p'), topic, file_type.lower())


def get_downloads(recording):
    downloads = []
    for download in recording['recording_files']:
        file_type = download['file_type']
        download_url = download['download_url']
        downloads.append((file_type, download_url,))
    return downloads


def list_recordings(user_id):
    endpoint_url = API2_ENDPOINT_RECORDING_LIST1 + user_id + API2_ENDPOINT_RECORDING_LIST2

    cum_data = []
    i: int = 0

    # as of now, zoom api only allows retrieval of records from the last 6 months, and in 1 month chunks
    while i <= 7:

        time_offset = datetime.timedelta(days=30) * i
        fr = (datetime.datetime.now() - datetime.timedelta(days=30) - time_offset).strftime('%Y-%m-%d')
        too = (datetime.datetime.now() - time_offset).strftime('%Y-%m-%d')
        print(f'fetching recordings for time period {fr} to {too}')
        param_payload = {
            'page_size': 300,
            'from': fr,
            'to': too
        }
        response = request_get(endpoint_url, params=param_payload)

        recordings_data = response.json()
        period_records = recordings_data['total_records']
        page_count = recordings_data['page_count']

        _recordings: list = recordings_data['meetings']
        while next_page_url := recordings_data['next_page_token'] != "":
            np = request_get(next_page_url)
            _recordings.extend(np.json())
        print(f'{len(_recordings)} recordings found for this time period')
        cum_data.extend(_recordings)
        i += 1

    return cum_data
    # recordings = recordings_data['meetings']

    # for i in range(1, page_count):  # start at page index 1 since we already have the first page
    #     current_page = i + 1
    #     print('Getting page {} of {}'.format(current_page, page_count))
    #     # post_data = get_credentials(user_id, current_page)
    #     param_payload = {
    #         'page_size': 300,
    #         'page_number': current_page
    #     }
    #     response = request_get(API_ENDPOINT_RECORDING_LIST, params=param_payload )
    #     recordings_data = response.json()
    #     if recordings_data:
    #         recordings.extend(recordings_data['meetings'])


# return recordings


def download_recording(download_url, email, filename):
    dl_dir = os.sep.join([DOWNLOAD_DIRECTORY, email])
    full_filename = os.sep.join([dl_dir, filename])
    try:
        if os.path.exists(full_filename):
            print(f'{filename} for account {email} already exists, skipping')
            return True
    except FileNotFoundError:
        # this is expected behavior
        pass
    os.makedirs(dl_dir, exist_ok=True)
    j = create_jwt()
    params_payload = {
        'access_token': j
    }
    response = request_get(download_url, params=params_payload, stream=True, no_jwt=True)
    if response.status_code != 200:
        raise ConnectionError
    try:

        with open(full_filename, 'wb') as fd:
            for chunk in response.iter_content(chunk_size=128):
                fd.write(chunk)
        return True
    except Exception as e:
        # if there was some exception, print the error and return False
        print(e)
        return False


def load_completed_meeting_ids():
    try:

        with open(COMPLETED_MEETING_IDS_LOG, 'r') as fd:
            for line in fd:
                COMPLETED_MEETING_IDS.add(line.strip())

    except FileNotFoundError as ex:
        with open(COMPLETED_MEETING_IDS_LOG, 'w') as f:
            print('', file=f)


def main():
    if not get_config_vars():
        # if we can't configure, exit
        print('not api authentication information available, quitting...')
        exit()
    load_completed_meeting_ids()

    users = get_user_ids()
    for user_id, email in users:
        print('Getting recording list for {}'.format(email))
        recordings = list_recordings(user_id)
        total_count = len(recordings)
        print('Found {} recordings'.format(total_count))

        for index, recording in enumerate(recordings):
            success = False
            meeting_id = recording['uuid']
            # import ipdb;ipdb.set_trace()
            if meeting_id in COMPLETED_MEETING_IDS:
                print('Skipping already downloaded meeting: {}'.format(meeting_id))
                continue

            downloads = get_downloads(recording)
            for file_type, download_url in downloads:
                filename = format_filename(recording, file_type)
                print('Downloading ({} of {}): {}: {}'.format(index + 1, total_count, meeting_id, download_url))
                success |= download_recording(download_url, email, filename)
                # success = True

            if success:
                # if successful, write the ID of this recording to the completed file
                with open(COMPLETED_MEETING_IDS_LOG, 'a') as log:
                    COMPLETED_MEETING_IDS.add(meeting_id)
                    log.write(meeting_id)
                    log.write('\n')
                    log.flush()


if __name__ == "__main__":
    main()
