#!/bin/bash
set -euo errexit
set -o pipefail

# check if kubectl is installed
if ! [ -x "$(command -v kubectl)" ]; then
  echo 'Error: kubectl is not installed.' >&2
  exit 1
fi

# check if gh is installed
if ! [ -x "$(command -v gh)" ]; then
  echo 'Error: gh (github cli) is not installed.' >&2
  exit 1
fi

# Usage function (optional, for clarity)
usage() {
  echo "Usage: ./k8s-rds-secrets-to-github-secrets.sh <kubernetes_namespace_root> <github_environment_name>"
}

# Process arguments and handle dry run
DRY_RUN=false
while getopts ":d" opt; do
  case ${opt} in
    d) DRY_RUN=true 
       ;;
    \?) echo "Invalid option: -$OPTARG" >&2
        usage
        exit 2
        ;;  
  esac
done
shift $((OPTIND -1))

# Check for required arguments
if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

GITHUB_REPO="ministryofjustice/data-catalogue"
RDS_SECRET_NAME="rds-postgresql-instance-output"
OS_SECRET_NAME="data-catalogue-opensearch-proxy-url"

GITHUB_ENV_NAME="$2"
KUBE_NAMESPACE="$1-$GITHUB_ENV_NAME"

# Get Kubernetes secret YAML and decode
RDS_SECRET_YAML=$(kubectl -n "$KUBE_NAMESPACE" get secret $RDS_SECRET_NAME -o yaml)

# Extract values from YAML (replacing Ruby logic)
RDS_INSTANCE_ADDRESS=$(echo "$RDS_SECRET_YAML" | grep 'rds_instance_address:' | awk '{print $2}' | base64 -d)
RDS_INSTANCE_ENDPOINT=$(echo "$RDS_SECRET_YAML" | grep 'rds_instance_endpoint:' | awk '{print $2}' | base64 -d)
DATABASE_USERNAME=$(echo "$RDS_SECRET_YAML" | grep 'database_username:' | awk '{print $2}' | base64 -d)
DATABASE_PASSWORD=$(echo "$RDS_SECRET_YAML" | grep 'database_password:' | awk '{print $2}' | base64 -d)
DATABASE_NAME=$(echo "$RDS_SECRET_YAML" | grep 'database_name:' | awk '{print $2}' | base64 -d)
RDS_URL="jdbc:postgresql://${RDS_INSTANCE_ENDPOINT}/${DATABASE_NAME}"

OS_SECRET_YAML=$(kubectl -n "$KUBE_NAMESPACE" get secret $OS_SECRET_NAME -o yaml)
OS_PROXY_URL=$(echo "$OS_SECRET_YAML" | grep 'proxy_url:' | awk '{print $2}' | base64 -d)
OS_PROXY_URL=$(echo "$OS_PROXY_URL" | sed -E 's|^https?://||' | sed -E 's|:[0-9]+$||')

if [[ $DRY_RUN == true ]]; then
  echo "** DRY RUN **"
  echo "KUBE_NAMESPACE: $KUBE_NAMESPACE"
  echo "Would set GitHub secrets:"
  echo "- POSTGRES_CLIENT_HOST=$RDS_INSTANCE_ADDRESS"
  echo "- POSTGRES_HOST=$RDS_INSTANCE_ENDPOINT"
  echo "- POSTGRES_URL=$RDS_URL"
  echo "- POSTGRES_USERNAME=$DATABASE_USERNAME"
  echo "- POSTGRES_PASSWORD=$DATABASE_PASSWORD"
  echo "- POSTGRES_DB_NAME=$DATABASE_NAME"
  echo "- OPENSEARCH_PROXY_HOST=$OS_PROXY_URL"
else
  # Set GitHub secrets (assuming you have GitHub CLI installed and configured)
  gh secret set POSTGRES_CLIENT_HOST \
    --body $RDS_INSTANCE_ADDRESS \
    --env $GITHUB_ENV_NAME \
    --repo $GITHUB_REPO 
  gh secret set POSTGRES_HOST \
    --body $RDS_INSTANCE_ENDPOINT \
    --env $GITHUB_ENV_NAME \
    --repo $GITHUB_REPO 
  gh secret set POSTGRES_URL \
    --body $RDS_URL \
    --env $GITHUB_ENV_NAME \
    --repo $GITHUB_REPO 
  gh secret set POSTGRES_USERNAME \
    --body $DATABASE_USERNAME \
    --env $GITHUB_ENV_NAME \
    --repo $GITHUB_REPO
  gh secret set POSTGRES_PASSWORD \
    --body $DATABASE_PASSWORD \
    --env $GITHUB_ENV_NAME \
    --repo $GITHUB_REPO
  gh secret set POSTGRES_DB_NAME \
    --body $DATABASE_NAME \
    --env $GITHUB_ENV_NAME \
    --repo $GITHUB_REPO
  gh secret set OPENSEARCH_PROXY_HOST \
    --body $OS_PROXY_URL \
    --env $GITHUB_ENV_NAME \
    --repo $GITHUB_REPO 
fi

exit 0
