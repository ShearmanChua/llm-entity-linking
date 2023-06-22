import json
import os
import pandas as pd
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

from utils.ESManager import DocManager
# from utils.WeaviateManager import VectorManager
from doc_utils.target import Target
from doc_utils.data import Data
from elasticsearch import Elasticsearch, helpers

TARGET_COLLECTION_NAME = "targets"
ARTICLES_COLLECTION_NAME = "articles"
DATA_ROOT = '../data'

if __name__ == '__main__':
    DocMgr = DocManager()
    # VecMgr = VectorManager()

    # Wikipedia Target Reference Schema
    with open(os.path.join(DATA_ROOT, 'es_target_schema.json')) as f:
        target_map = json.load(f)
    target_map = target_map['targets']['mappings']
    DocMgr.create_collection(TARGET_COLLECTION_NAME, schema=target_map, custom_schema=True)

    target_path = os.path.join(DATA_ROOT, 'wiki_10seed_1hop.csv')
    TargetMgr = Target(target_path)
    for collated_docs in TargetMgr.ingest():
        DocMgr.create_document(TARGET_COLLECTION_NAME, collated_docs, id_field=None)

    # Article Schema
    with open(os.path.join(DATA_ROOT, 'es_articles_schema.json')) as f:
        article_map = json.load(f)
    article_map = article_map['articles']['mappings']
    DocMgr.create_collection(ARTICLES_COLLECTION_NAME, schema=article_map, custom_schema=True)

    data_files = ['cnn.csv','author-parse-articles-filtered.csv', 'Guardians_Russia_Ukraine.csv', 'NYT_Russia_Ukraine.csv', 'reuters.csv', 'theguardian.csv']
    for file in data_files:
        print(file)
        DataMgr = Data(os.path.join(DATA_ROOT, 'articles', file))
        for collated_docs in DataMgr.ingest():
            DocMgr.create_document(ARTICLES_COLLECTION_NAME, collated_docs, id_field=None)
