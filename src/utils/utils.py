import pandas as pd
import yaml
from tqdm import tqdm
from typing import Dict
import json


def read_yaml(file_path='config.yaml'):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)