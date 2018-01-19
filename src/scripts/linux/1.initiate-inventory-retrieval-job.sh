#!/bin/sh

# https://docs.aws.amazon.com/cli/latest/reference/glacier/index.html

VAULT=$1

aws glacier initiate-job --account-id - --vault-name $VAULT --job-parameters '{"Type": "inventory-retrieval"}'
