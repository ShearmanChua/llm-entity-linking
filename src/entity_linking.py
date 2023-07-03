import ast
import json
import requests
from datetime import date

from utils.ESManager import DocManager
from utils import utils
from tqdm import tqdm
import pandas as pd
import numpy as np

def link_entities(text, entities,cfg):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    # entities = [entity[3] for entity in entities]
    payload = json.dumps({"text":text, "entities":entities})
    response = requests.get(cfg["el_endpoint"], data=payload, headers=headers)

    return json.loads(response.text)['result']

def entity_linking():
    cfg = utils.read_yaml("../configs/configs.yaml")
    es_manager = DocManager()

    documents = es_manager.get_all_documents(collection_name=cfg['elasticsearch']['articles_index'])
    articles_df = pd.DataFrame(columns=['title','text','elasticsearch_ID','entities'])

    for doc in tqdm(documents):
        if "linked_entities.links" not in doc['_source'].keys():
            ner_types = ['locations','organizations','others','people']
            entities = []
            for type in ner_types:
                entities.extend(doc['_source']['entities.{}'.format(type)])
            articles_df.loc[-1] = [doc['_source']['title'], doc['_source']['content'], doc['_id'],entities]  # adding a row
            articles_df.index = articles_df.index + 1  # shifting index
            articles_df = articles_df.sort_index()  # sorting by index

    # ner_df = pd.read_csv("../data/ner_results_articles.csv", index_col=False)
    # ner_df['predictions'] = ner_df['predictions'].apply(lambda x: ast.literal_eval(str(x)))
    # ner_df.drop(columns=['elasticsearch_ID'], inplace=True)
    # print(ner_df.head())

    # articles_df = pd.concat([articles_df,ner_df], axis=1, join="inner",keys=['text'])

    for idx, row in tqdm(articles_df.iterrows(),total=len(articles_df.index)):
        index = row["elasticsearch_ID"]
        targets = link_entities(row["text"], row['entities'],cfg)
        print(row['entities'])
        print(targets)
        print("\n")
        es_manager.update_document(collection_name=cfg['elasticsearch']['articles_index'], doc_id=index, document={"linked_entities":{"links":targets,"mentions":row['entities']}})

if __name__ == '__main__':
    entity_linking()