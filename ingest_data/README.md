
## Directions for launching flows from this dir

0. Activate venv if not using docker

`conda activate <env>`

1. Prefect Task Runner

`prefect orion start`
`prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api`
Vist http://127.0.0.1:4200/


`prefect block register -m prefect_gcp`

<create GCS credentials block, using service account creds>
GCP > IAM > Service Accounts

2. Extract and transform 

`python web_to_gcs.py`