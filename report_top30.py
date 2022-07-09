#!/usr/bin/env python
# coding: utf-8
from configs import *
import pandas as pd
import os
import owncloud
import urllib3
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import date

# PATH 
path = r'<replaced>'
os.chdir(path)

# OwnCloud
REMOTE_DIR = '<replaced>'

# LOGGING CONFIGS
for _ in logging.root.manager.loggerDict:
    logging.getLogger(_).setLevel(logging.CRITICAL)
    
log_file = "LOG.txt"
log_file = logging.FileHandler(log_file, 'a')
streamhl = logging.StreamHandler()
logging.basicConfig(handlers=[log_file, streamhl], level=logging.INFO,
                    format='[%(asctime)s: %(levelname)s] %(message)s',
                    datefmt='%d-%m-%Y %H:%M:%S')

#ELASTICSEACERCH
es = Elasticsearch([{'host': '<replaced>', 'port': '<replaced>'}])
index = "<replaced>"


def send_report(remote_dir, file, user, password):
    # Auth OwnCloud
    urllib3.disable_warnings()
    oc = owncloud.Client('<replaced>', verify_certs=False)
    oc.login(user, password)
    
    # Send report
    path = remote_dir + "/" +  file
    oc.put_file(path, file)


#Запрос по резюме
query_cv = {
      "query": {
        "query": {
          "bool": {
            "must": [
              {
                "term": {
                  "deleted": "0"
                }
              },
              {
                "term": {
                  "moderated": "1"
                }
              },
              {
                "term": {
                  "visible": "1"
                }
              }
            ],
            "must_not": [],
            "should": []
          }
        }
      },
      "size": 0,
      "aggs": {
        "2": {
          "terms": {
            "field": "<replaced>",
            "size": 30,
            "order": {
              "_count": "desc"
            }
          }
        }
      }
    }


def main():
    # Start
    logging.info('Start')
    
    # Query
    result = es.search(index=index, body=query_cv, request_timeout=50)
    logging.info('Executed.')
    
    # Get
    df = pd.DataFrame(result['aggregations']['2']['buckets'])
    df.columns = ['<replaced>']
    logging.info('Got it.')
    
    # Save
    date_rep = date.today().strftime('%Y-%m-%d')
    file = f'<replaced> ({date_rep}).xlsx'
    df.to_excel(file, index=False)
    logging.info(f'Saved: {file}.')
    
    # Send
    send_report(REMOTE_DIR, file, USER_OC, PASS_OC)
    logging.info('Sent.')
    logging.info('End.')
    log_file.close()


if __name__ == '__main__':
    main()
