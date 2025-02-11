#!/bin/bash

echo "Getting secrets from ${SNOWFLAKE_SECRET}..."
SNOWFLAKE_SECRET=$(aws secretsmanager get-secret-value --secret-id ${SNOWFLAKE_SECRET})
SNOWFLAKE_SECRET=$(echo $SNOWFLAKE_SECRET | jq ".SecretString" | sed -e 's/\\r//g' -e 's/\\n//g' -e 's/\\//g' -e 's/"{/{/g' -e 's/}"/}/g')

echo "Getting secrets from ${WORKDAY_NONPII_KEYS}..."
WORKDAY_NONPII_KEYS=$(aws secretsmanager get-secret-value --secret-id ${WORKDAY_NONPII_KEYS})
WORKDAY_NONPII_KEYS=$(echo $WORKDAY_NONPII_KEYS | jq ".SecretString" | sed -e 's/\\r//g' -e 's/\\n//g' -e 's/\\//g' -e 's/"{/{/g' -e 's/}"/}/g')

echo "Getting secrets from ${WORKDAY_PII_KEYS}..."
WORKDAY_PII_KEYS=$(aws secretsmanager get-secret-value --secret-id ${WORKDAY_PII_KEYS})
WORKDAY_PII_KEYS=$(echo $WORKDAY_PII_KEYS | jq ".SecretString" | sed -e 's/\\r//g' -e 's/\\n//g' -e 's/\\//g' -e 's/"{/{/g' -e 's/}"/}/g')

cd rsi_wfa_python/

echo "Executing: python $* from entrypoint.sh"
python $*