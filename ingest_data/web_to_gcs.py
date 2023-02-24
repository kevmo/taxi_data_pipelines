from pathlib import Path 

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3)
def fetch(dataset_url):
    """Read taxi data from web into pd DataFrame"""
    df = pd.read_csv(dataset_url)
    return df 


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print("------------LOGGIN'----------------------")
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}")
    
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)

    df.to_parquet(path, compression="gzip")
    return path 

@task(log_prints=True)
def write_gcs(path:Path) -> None:
    """Upload local Parquet file to GCS"""
    gcs_block = GcsBucket.load("zoomcamp-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=500)
    return


@flow(log_prints=True)
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2020

    # month = 1
    # dataset_file = f"{color}_tripdata_{year}-{month:02}"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    # dataset_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-01.csv.gz"
    # dataset_file = "green_tripdata_2020-01"

    months = list(range(1,13))

    for month in months:

        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"


        df = fetch(dataset_url)
        df_clean = clean(df)
        path = write_local(df_clean, color, dataset_file)
        write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
