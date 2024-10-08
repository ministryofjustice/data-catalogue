# DataHub metric query instructions

This readme explains the steps needed to run the datahub metric query locally and show latest metadata completeness visualisations in a jupyter notebook.

It assumes you have:
1. Python installed on your local machine.
2. Are part of the `data-catalogue` github team.
3. Have the postgres RDS credentials - or access to 1password Data-Catalogue vault to get them.

## Step 1: Add variable to a `.env` file
Populate a `.env` file in the `datahub_metrics` folder

example for preprod - (swap out variables for secrets stored in 1password in the Datahub RDS Postgres instance category of the Data-Catalogue vault):

```
DATABASE_URL=postgresql+psycopg://{database_username}:{database_password}@localhost:1234/{database_name}
DATAHUB=preprod
```

Then run the below command in your terminal:
```
source datahub_metrics/.env
````

## Step 2: Create a virtual environment and install requirements
In your terminal run the commands:
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

if you already have the venv and requirements installed you can just run `source venv/bin/activate`

## Step 3: Setup port forwarding for postgres connection
You may need to create a port-forward pod if it does not already exist. Try running this command in your terminal (you will find RDS_INSTANCE_ADDRESS in 1password in the Datahub RDS Postgres instance category of the Data-Catalogue vault):
```
kubectl -n data-platform-datahub-catalogue-preprod \
run port-forward-pod \
--image=ministryofjustice/port-forward \
--port=5432 \
--env="REMOTE_HOST={RDS_INSTANCE_ADDRESS}" \
--env="LOCAL_PORT=5432" \
--env="REMOTE_PORT=5432"

```

Then open a new terminal on your macbook run the command `kubectl -n data-platform-datahub-catalogue-preprod port-forward port-forward-pod 1234:5432`. This will setup port forwarding to the prod postgres and will need to be running whilst you execute the query in following steps.

## Step 4: Start and run the jupyter notebook
From a different terminal than step 3 (step 3 needs to be running alongside) run the command:
```
jupyter-lab
```
This will open jupyterlab in your browser. Then open the notebook `datahub_metrics/metadata_metric_charts.ipynb` and from the top toolbar select `Run > Run All Cells`

You should see the charts at the bottom of the notebook.
