# dbt runner image for the financial-data-lakehouse gold step.
#
# Built once on the host:
#   docker build -t financial-data-lakehouse-dbt ./dbt_modeling
# and launched per-run by the Airflow DAG's `dbt_build` DockerOperator.
#
# The image only installs dbt + the Athena adapter. The project itself
# (models, profiles.yml, vendored dbt_packages) and the AWS creds are
# BIND-MOUNTED at runtime by the DAG:
#   <host>/dbt_modeling -> /usr/app/dbt_modeling   (project + profiles)
#   <host>/.aws         -> /root/.aws  (read-only)  (boto3/Athena creds)
# so you can iterate on models without rebuilding the image.
FROM python:3.12-slim

# dbt-core 1.12 / dbt-athena 1.11 are prerelease (see requirements.txt), so
# install with --pre. Copy only requirements to keep this layer cached across
# model changes (the project comes from the bind mount, not a COPY).
WORKDIR /usr/app/dbt_modeling
COPY requirements.txt .
RUN pip install --no-cache-dir --pre -r requirements.txt

# AWS_REGION + GOLD_BUCKET are injected by the DockerOperator's `environment=`;
# profiles.yml reads GOLD_BUCKET via env_var(). No CMD — the DAG passes the
# full `dbt build ...` command at run time.
