from time import sleep
from elasticsearch import Elasticsearch
import requests
import logging
import argparse

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
hd = logging.StreamHandler()
logger.addHandler(hd)

def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {
            "records": {
                "dynamic": "strict",
                "properties" : {
                    "created_on" : {
                        "type" : "date",
                        "format" : "iso8601"
                    },
                    "map_name" : {
                        "type" : "keyword"
                    },
                    "mode" : {
                        "type" : "keyword"
                    },
                    "points" : {
                        "type" : "long"
                    },
                    "replay_id" : {
                        "type" : "long"
                    },
                    "server_name" : {
                        "type" : "text"
                    },
                    "stage" : {
                        "type" : "integer"
                    },
                    "steamid64" : {
                        "type" : "keyword"
                    },
                    "teleports" : {
                        "type" : "long"
                    },
                    "tickrate" : {
                        "type" : "integer"
                    },
                    "time" : {
                        "type" : "double"
                    }
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            logger.info(f"Created index {index_name}")
        created = True
    except Exception as ex:
        logger.critical(str(ex))
    finally:
        return created

PROP_TO_GET = ['steamid64', 'server_name', 'created_on', 'stage', 'mode', 'tickrate', 'time', 'teleports', 'points', 'replay_id', 'map_name']

def get_record(id):
    for _ in range(10):

        resp = requests.get(f"https://kztimerglobal.com/api/v2/records/{id}", timeout=10)
        if (resp.status_code == 200):
            line_json = resp.json()
            if line_json is None:
                return None, None
            id = line_json['id']
            rec = {}
            #TODO: Improve the method of getting props below
            for prop in PROP_TO_GET:
                rec[prop] = line_json[prop]
            return id, rec
        sleep(0.7)

    logger.debug(f"Cannot get record from id {id}")
    return None, None

def main():
    parser = argparse.ArgumentParser(prog='kzcontinue', description='Continuously upload kz records to an elastic node')
    
    parser.add_argument('ip')
    parser.add_argument('port')
    parser.add_argument('index')
    parser.add_argument('start_id')
    parser.add_argument('--version', action='version', version='0.0.1')
    parser.add_argument('--verbose', '-v')

    args = parser.parse_args()

    if hasattr(parser, 'verbose'):
        verbosiy_level = parser.verbose
        try:
            verbosiy_level = int(verbosiy_level)
        except ValueError:
            verbosiy_level = verbosiy_level.upper()
        hd.setLevel(verbosity_level)
    
    start = int(args.start_id)
    es = Elasticsearch(hosts=[{'host': args.ip, 'port': int(args.port)}])
    logger.info(es.info())

    if es is not None:
        if create_index(es, 'kzapi'):
            while True:
                idx, rec = get_record(start)
                start += 1
                if rec is not None:
                    out = es.index(index=args.index, body=rec, id=idx)
                    logger.info(f"Data indexed successfully for run #{idx}")
    else:
        logger.error(f"Cannot retrieve data from {args.ip}:{args.port}")