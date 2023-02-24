
from pathlib import Path

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True)
def ingest_web_to_gcs(path):
    gcs_block = GcsBucket.load("zoomcamp-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path, timeout=600)
    return

@flow(log_prints=True)
def main_flow(month):
    # month = "01"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month}.csv.gz"
                    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz

    # dataset_url = Path("https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz")
    path = Path(f"data/fhv_tripdata_2019-{month}.csv.gz")
    ingest_web_to_gcs(path)

# TODO: lol make this a loop
if __name__ == "__main__":
    for month in ["01","02","03", "04","05","06", "07", "08", "09","10", "11", "12"]:
        main_flow(month)