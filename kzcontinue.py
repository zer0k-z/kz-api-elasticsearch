import sys
from time import sleep
from elasticsearch import Elasticsearch
import requests

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
            print(f"Created index {index_name}")
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created

def get_record(id):
    for _ in range(10):

        resp = requests.get(f"https://kztimerglobal.com/api/v2/records/{id}", timeout=10)
        if (resp.status_code == 200):
            line_json = resp.json()
            if line_json is None:
                return None, None
            id = line_json['id']
            rec = {'steamid64': line_json['steamid64'], 'server': line_json['server_name'], 'created_on': line_json['created_on'], 'stage': line_json['stage'], 'mode': line_json['mode'], 
                'tickrate': line_json['tickrate'], 'time': line_json['time'], 'teleports': line_json['teleports'], 'points': line_json['points'], 'replay_id': line_json['replay_id'], 'map_name':line_json['map_name']}
            sleep(0.6)
            return id, rec
        sleep(0.7)

    return None, None

if __name__ == '__main__':
    
    if len(sys.argv) < 5:
        print("Usage: python kzcontinue.py <ip> <port> <index> <start_id>")
        exit()

    start = int(sys.argv[4])
    es = Elasticsearch(hosts=[{'host': sys.argv[1], 'port': int(sys.argv[2])}])
    print(es.info())

    if es is not None:
        if create_index(es, 'kzapi'):
            while True:
                idx, rec = get_record(start)
                start += 1
                if rec is not None:
                    out = es.index(index=sys.argv[3], body=rec, id=idx)
                    print(f"Data indexed successfully for run #{idx}")
    else:
        print(f"Cannot retrieve data from {sys.argv[1]}:{sys.argv[2]}")