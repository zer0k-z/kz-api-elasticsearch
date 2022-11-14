import argparse
import logging
from time import sleep

from elasticsearch import Elasticsearch
from steam import webapi

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
hd = logging.StreamHandler()
logger.addHandler(hd)

def create_query_associate_id(steamid, sname):
  query = {
    "query": {
      "term": {
        "steamid64.keyword": f"{steamid}"
      }
    },
    "script" : {
      "id": "update-name",
      "params": {
          "name": sname
      }
    }
  }
  return query

REQUEST = """{"size":0,"aggs":{"my_buckets":{"composite":{"size":100,"sources":[{"steamid":{"terms":{"field":"steamid64.keyword"}}}],"after":{"steamid":"0"}}}}}"""
REQUEST_PAGINATED = """{"size":0,"aggs":{"my_buckets":{"composite":{"size":100,"sources":[{"steamid":{"terms":{"field":"steamid64.keyword"}}}],"after":{"steamid":"REPLACE_STEAM_ID_HERE"}}}}}"""

def main():
    parser = argparse.ArgumentParser(prog='elastic-steam-name', description='Continuously update steamid name from an elastic node')
    
    parser.add_argument('url')
    parser.add_argument('index')
    parser.add_argument('steam_webapi_key')
    parser.add_argument('--verbose', '-v')
    parser.add_argument('--version', action='version', version='0.0.3')

    args = parser.parse_args()

    if hasattr(parser, 'verbose'):
        verbosity_level = parser.verbose
        try:
            verbosity_level = int(verbosity_level)
        except ValueError:
            verbosity_level = verbosity_level.upper()
        hd.setLevel(verbosity_level)

    es = Elasticsearch(hosts=[args.url])
    es.put_script(id="update-name", body='{"script":{"lang":"painless", "source":"ctx._source.player_name = params[\'name\']"}}')
    logger.info(es.info())
    steam_webapi_client = webapi.WebAPI(args.steam_webapi_key)
    resp = es.search(index=args.index, size = 0, body = REQUEST)

    if es is not None:
      while True:
        while 'after_key' in resp['aggregations']['my_buckets']:
          try:
              steamid = int(resp['aggregations']['my_buckets']['after_key']['steamid'])
              resp = es.search(index=args.index, size = 0, body=REQUEST_PAGINATED.replace("REPLACE_STEAM_ID_HERE", str(steamid)))

              steamid_query = ""
              for entry in resp['aggregations']['my_buckets']['buckets']:
                  steamid_query = steamid_query + entry['key']['steamid'] + ","
              for _ in range(0,10):
                while True:
                    try:
                        result = (steam_webapi_client.ISteamUser.GetPlayerSummaries(steamids=steamid_query[:-1]))
                    except:
                        sleep(1)
                        continue
                    break
              for player in result['response']['players']:
                  logger.info(f"{player['steamid']} - {player['personaname']}")
                  es.update_by_query(body=create_query_associate_id(player['steamid'],player['personaname']), index=args.index)
          except Exception as e:
              logger.error(e)
              break
        sleep(86400)
        resp = es.search(index=args.index, size = 0, body = REQUEST)

