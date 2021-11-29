import argparse

import requests
import json


def get_community_records():

    response = requests.get('https://zenodo.org/api/records?communities=spi-ace&page=1&size=200&access-token=aNLSrPHSZY0JEjLSdIpflZIjSpvjYzKNVvfPTq7xdhihtvNn0KoII7yt8vEN&all-version=1')

    records = response.json()

    return records


def read_json_file(json_file):

    with open(json_file) as json_file:
        data = json.load(json_file)

    return data







if __name__== "__main__":
    parser = argparse.ArgumentParser(description="Read JSON file")
    parser.add_argument("json_file", help="JSON file")

    args = parser.parse_args()

    records = read_json_file(args.json_file)
    print(records)