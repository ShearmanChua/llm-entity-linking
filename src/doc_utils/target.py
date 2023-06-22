import pandas as pd
from tqdm import tqdm

# from utils.IdGeneration import IDManager


MAX_BULK_SIZE=100

class Target:
    def __init__(self, file_path):
        self.data = pd.read_csv(file_path, index_col=False)
        # self.IdMgr = IDManager()
        

    def ingest(self):
        """
        A generator function that batches lines within the dataset and return the batch
        """
        total_docs = len(self.data)
        # all_id = self.IdMgr.generate(total_docs) # Generate n number of unique ids
        # self.data['_id'] = list(all_id)
        docs = []
        for i, rows in tqdm(self.data.iterrows(), total=total_docs):
            doc = {
                # "_id": rows["_id"],
                "title": rows['title'],
                "content": str(rows['content']),
                "retrieved": str(rows['retrieved']),
                "revision_id": str(rows['revision_id']),
                "wiki_id": str(rows['id'])  
            }
            docs.append(doc)
            if len(docs) == MAX_BULK_SIZE:
                yield docs
                docs = []
        if len(docs): # Return remaining docs 
            yield docs

    def ingest_single(self):
        """
        A generator function that batches lines within the dataset and return the batch
        """
        total_docs = len(self.data)
        # all_id = self.IdMgr.generate(total_docs) # Generate n number of unique ids
        # self.data['_id'] = list(all_id)
        for i, rows in tqdm(self.data.iterrows(), total=total_docs):
            doc = {
                # "_id": rows["_id"],
                "title": rows['title'],
                "content": str(rows['content']),
                "retrieved": str(rows['retrieved']),
                "revision_id": str(rows['revision_id']),
                "wiki_id": str(rows['id'])
            }
            yield doc