import argparse

import requests
import json
import csv
import sys

def get_community_records():

    response = requests.get('https://zenodo.org/api/records?communities=spi-ace&page=1&size=200&access-token=aNLSrPHSZY0JEjLSdIpflZIjSpvjYzKNVvfPTq7xdhihtvNn0KoII7yt8vEN&all-version=1')

    records = response.json()

    return records


def read_json_file(json_file):

    with open(json_file) as json_file:
        data = json.load(json_file)
    print(data.keys())
    return data


def get_conceptdoi_data(data):

    conceptdois = [] # list of all ConceptDOIs
    versions = 0 # total number of different DOI versions
    doi_details = []

    for hit in data['hits']['hits']:
        conceptdois.append(hit['conceptdoi'])
        versions += hit['metadata']['relations']['version'][0]['count'] # get the number of versions for each concept DOI

        doi_details.append((hit['conceptdoi'], hit['metadata']['relations']['version'][0]['count']))

    print("There are", len(conceptdois), "concept DOIs")
    print("with", versions, "versions")

    return doi_details


def print_line(csv_out, hit, doi):
    csv_out.writerow([hit['conceptdoi'], doi, hit['metadata']['relations']['version'][0]['count'], hit['metadata']['title']])


def get_multiple_version_dois(data):
    csv_file = csv.writer(sys.stdout)

    multiple_version_dois = []

    for hit in data['hits']['hits']:
        multiple_version_dois.append((hit['conceptdoi'], hit['conceptdoi']))
        csv_file.writerow([hit['conceptdoi'], hit['conceptdoi'], hit['metadata']['relations']['version'][0]['count'], hit['metadata']['title']])
        multiple_version_dois.append((hit['conceptdoi'], hit['doi']))

        versions = hit['metadata']['relations']['version'][0]['count']

        while versions > 1:
            print_line(csv_file, hit, '')
            versions -= 1

        print_line(csv_file, hit, hit['doi'])

    return multiple_version_dois


if __name__== "__main__":
    parser = argparse.ArgumentParser(description="Read JSON file")
    parser.add_argument("json_file", help="JSON file")

    args = parser.parse_args()

    records = read_json_file(args.json_file)
    doi_details = get_conceptdoi_data(records)

    print(sorted(doi_details, key=lambda t:t[1], reverse=True))
    print("-------------------------------\n\n")

    print("-------------------------------\n\n")

    multiple_version_dois = get_multiple_version_dois(records)


    #print("Records with only one version:", one_version_dois)
    #print("-------------------------------\n\n")

    #print("Records with multiple versions: ", multiple_version_dois)
