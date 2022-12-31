import argparse
import logging
import time
from time import sleep

import requests
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')
hd = logging.StreamHandler()
hd.setFormatter(formatter)
logger.addHandler(hd)

def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "records": {
                "dynamic": "true",
                "properties" : {
                    "id" : {
                        "type": "integer"
                    },
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
                    },
                    "player_name": {
                        "type" : "keyword"
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

PROP_TO_GET = ['id','steamid64', 'server_name', 'created_on', 'stage', 'mode', 'tickrate', 'time', 'teleports', 'map_name', 'player_name']

def get_record(id):
    for _ in range(10):
        # Conservative request limit at 100/minute to not get 429'd.
        sleep(0.6)
        resp = requests.get(f"https://kztimerglobal.com/api/v2/records/{id}", timeout=10)
        if (resp.status_code == 200):
            line_json = resp.json()
            if line_json is None:
                return None, None
            id = line_json['id']
            rec = {}
            #TODO: Improve the method of getting props below
            for prop in PROP_TO_GET:
                if prop in line_json:
                    rec[prop] = line_json[prop]
            return id, rec

    logger.debug(f"Cannot get record from id {id}, status code {resp.status_code}, content {resp.json()}")
    return None, None

def main():
    parser = argparse.ArgumentParser(prog='kzcontinue', description='Continuously upload kz records to an elastic node')
    
    parser.add_argument('url')
    parser.add_argument('index')
    parser.add_argument('--version', action='version', version='0.0.5')
    parser.add_argument('--verbose', '-v')
    parser.add_argument('--timeout', type=int)

    args = parser.parse_args()
    if not args.timeout:
        timeout = 600
    else:
        timeout = args.timeout

    if hasattr(parser, 'verbose'):
        verbosity_level = parser.verbose
        try:
            verbosity_level = int(verbosity_level)
        except ValueError:
            verbosity_level = verbosity_level.upper()
        hd.setLevel(verbosity_level)

    es = Elasticsearch(hosts=[args.url])
    logger.info(es.info())

    if es is not None:
        if create_index(es, args.index):
            current = 0
            try:
                resp = es.search(index=args.index,size=1, sort=[{"created_on":{"order":"desc"}}])
                current = int(resp['hits']['hits'][0]['_id']) + 1
                logger.info(f"Latest index: #{current - 1}")
            except Exception as e:
                logger.info(f"Exception: {e}")
                pass
            success = False
            last_success_time = time.time()
            while (time.time() - last_success_time) < timeout:
                idx, rec = get_record(current)
                success = False
                if rec is not None:
                    current += 1
                    es.index(index=args.index, body=rec, id=idx)
                    logger.info(f"Data indexed successfully for run #{current}")
                    success = True
                    last_success_time = time.time()
                else:
                    # Look for a few runs ahead, just in case we encounter a null run instead of a run that doesn't exist yet
                    for i in range(1,25):
                        idx, rec = get_record(current+i)
                        if rec is not None:
                            es.index(index=args.index, body=rec, id=idx)
                            logger.info(f"Data indexed successfully for run #{current+i}")
                            success = True
                            last_success_time = time.time()
                            # Double check the past runs to really make sure it is a null run 
                            # and wasn't created while we send the last few requests.
                            for j in range(0,i):
                                idx, rec = get_record(current+j)
                                if rec is not None:
                                    logger.info(f"Data indexed successfully for run #{current+j}")
                                else:
                                    rec = {"id": current+j}
                                    logger.info(f"Null run insert for run #{current+j}")
                                es.index(index=args.index, body=rec, id=idx)
                            current += i
                            break
                # No run in sight, pause for 1 minute.
                if not success:
                    sleep(60)
            logger.error(f"API is unresponsive, shutting down. Last runID: {current}")
    else:
        logger.error(f"Cannot retrieve data from {args.ip}:{args.port}")