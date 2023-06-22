import pandas as pd
from tqdm import tqdm

MAX_BULK_SIZE=100

class Data:
    def __init__(self, file_path) -> None:
        self.fp = file_path.split('/')[-1]
        self.df = pd.read_csv(file_path, index_col=False)

    def _check_fields(self):
        """
        Adds empty string to column if field does not exist
        """
        required_fields = ['title', 'link', 'content']
        doc = {}
        for field in required_fields:
            if not field in self.df:
                self.df[field]=[""]*len(self.df)
        return
                

    def ingest(self):
        """
        A generator function that batches lines within the dataset and return the batch
        """
        total_docs = len(self.df)
        docs = []
        self._check_fields()
        for i, rows in tqdm(self.df.iterrows(), total=total_docs):
            doc = {
                "title": str(rows['title']),
                "link": str(rows['link']),
                "content": str(rows['content']),
                "file_source": str(self.fp)
            }
            if not pd.isna(rows['timestamp_year']):
                timestamp_date = f"{int(rows['timestamp_year']):04}-{int(rows['timestamp_month']):02}-{int(rows['timestamp_day']):02}"
                timestamp_time = f"{int(rows['timestamp_hour']):02}:{int(rows['timestamp_minute']):02}:00"
                timestamp = f"{timestamp_date}T{timestamp_time}Z"
                doc['timestamp'] = str(timestamp)
            
            docs.append(doc)
            if len(docs) == MAX_BULK_SIZE:
                yield docs
                docs = []
        if len(docs): # Return remaining docs 
            yield docs