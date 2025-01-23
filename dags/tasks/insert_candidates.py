import clickhouse_connect
from dotenv import dotenv_values
import pathlib
import requests
from create_table import get_clickhouse_client
import json 
import datetime

def insert_candidates(client, no_rows):
    count = 0
    while count < no_rows:
        url = "https://randomuser.me/api"
        response = requests.get(url + "?nat=us").text    # About 90 requests per minute
        json_value = json.loads(response)
        data = json_value['results'][0]
        if data['dob']['age'] < 22:
            continue
        id = data['login']['uuid']
        first_name = data['name']['first']
        last_name = data['name']['last']
        full_name = first_name + ' ' +last_name
        street = str(data['location']['street']['number']) + ' ' + data['location']['street']['name']
        city = data['location']['city']
        state = data['location']['state']
        country = data['location']['country']
        postcode = str(data['location']['postcode'])
        email = data['email']
        date_of_birth = str(datetime.datetime.strptime(data['dob']['date'], '%Y-%m-%dT%H:%M:%S.%fZ').date())
        age = int(data['dob']['age'])
        phone_number = data['phone']
        picture_url = data['picture']['large']
        national_id = data['nat']
        client.insert('voting_system.candidates', [(id, first_name, last_name, full_name, street, city, state, country, postcode, email, date_of_birth, age, phone_number, picture_url, national_id)])
        count += 1
        
        

if __name__ == '__main__':
    script_path = pathlib.Path(__file__).parent.resolve()
    config = dotenv_values(f'{script_path.parent.parent}/.env')
    client = get_clickhouse_client(config)
    results = client.query('select * from voting_system.candidates')
    no_rows = len(results.result_rows)
    if no_rows == 0:
        insert_candidates(client, 3)
    elif no_rows < 3 and no_rows > 0:
        insert_candidates(client, 3- no_rows)

    # print(type(results.result_rows))    # Return list type