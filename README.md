# Pipeline prototype

* This pipeline uses AppEngine, PubSub and BigQuery

* The app retrieves messages from `PUBSUB_SUBSCRIPTION` and writes them to `BIGQUERY_TABLE`.

## Usage

### Authenticate

#### Create service account

1. create the service account

        gcloud iam service-accounts create [NAME]

2. Grant permissions to the service account
        gcloud projects add-iam-policy-binding [PROJECT_ID] \
        --member "serviceAccount:[NAME]@[PROJECT_ID].iam.gserviceaccount.com" \
        --role "roles/owner"`

3. Generate key file
        gcloud iam service-accounts keys create service-account.json \
        --iam-account [NAME]@[PROJECT_ID].iam.gserviceaccount.com`

### Run Locally

1. If you don't have `virtualenv`, install using pip

        sudo pip install virtualenv

2. Create isolated **Python** environment, and install dependencies

        virtual env
        source env/bin/activate
        pip install -r requirements.txt

3. Export environmental variables

        export GOOGLE_APPLICATION_CREDENTIALS="./service-account.json"
        export PROJECT_ID=knowledge-prototype
        export PUBSUB_TOPIC=cyton-data
        export PUBSUB_SUBSCRIPTION=cyton-data

4. Run `python main.py`

### Run App Engine

