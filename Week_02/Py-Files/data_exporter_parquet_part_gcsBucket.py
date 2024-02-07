from google.cloud import storage 
from pandas import DataFrame
import os
import pyarrow as pa 
import pyarrow.parquet as pq 

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/src/service_acc_2.json"


bucket_name = 'mage-homework-02'
project_id = 'datatalks-de-409612'

table_name = "green_taxi_hw_02"

path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:

    table = pa.Table.from_pandas(df)

    fs = pa.fs.GcsFileSystem()

    
    pq.write_to_dataset(
        table,
        root_path=path,
        partition_cols=['lpep_pickup_date'],
        filesystem=fs
    )