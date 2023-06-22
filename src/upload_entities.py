import ast
import json
import requests
from datetime import date

from utils.ESManager import DocManager
from utils import utils
from tqdm import tqdm
import pandas as pd
import numpy as np

def get_entities(text, cfg):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    payload = json.dumps({"text":text})
    response = requests.get(cfg["ner_endpoint"], data=payload, headers=headers)

    return json.loads(response.text)['results']

def upload_ner():
    cfg = utils.read_yaml("../configs/configs.yaml")
    es_manager = DocManager()

    documents = es_manager.get_all_documents(collection_name=cfg['elasticsearch']['articles_index'])
    articles_df = pd.DataFrame(columns=['TITLE','TEXTCONTENT','elasticsearch_ID'])

    for doc in tqdm(documents):
        
        if "entities.quantities" not in doc['_source'].keys():
            print(doc['_source'].keys())
            articles_df.loc[-1] = [doc['_source']['title'], doc['_source']['content'], doc['_id']]  # adding a row
            articles_df.index = articles_df.index + 1  # shifting index
            articles_df = articles_df.sort_index()  # sorting by index

    for idx, row in tqdm(articles_df.iterrows(),total=len(articles_df.index)):
        index = row["elasticsearch_ID"]
        entities = get_entities(row["TEXTCONTENT"], cfg)
        es_manager.update_document(collection_name=cfg['elasticsearch']['articles_index'], doc_id=index, document=entities)

if __name__ == '__main__':
    upload_ner()