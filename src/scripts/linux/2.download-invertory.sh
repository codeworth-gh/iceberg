#!/bin/sh

# https://docs.aws.amazon.com/cli/latest/reference/glacier/index.html

VAULT=$1
JOB=$2

aws glacier get-job-output --account-id - --vault-name $VAULT --job-id $JOB $VAULT.inventory.json

# cat $VAULT.inventory.json | python -mjson.tool > $VAULT.inventory.formatted.json
