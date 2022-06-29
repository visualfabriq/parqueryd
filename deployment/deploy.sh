#!/usr/bin/env bash

set -e

##### Variables section #####
ACCEPTABLE_STATUSES=("Pending" "InProgress")
FINAL_STATUS="Success"
REFRESH_RATE=60
#############################

##### Functions section #####
get_invocation_status() {
	aws ssm list-command-invocations --command-id "$1" --query "CommandInvocations[0].[Status]" | jq -r ".[0]"
}

##### Execution section #####
TARGETS=$(aws ec2 describe-instances --filters "Name=tag:ServiceName,Values=${SERVICE_NAME}" | jq '[.Reservations[].Instances[0].InstanceId]')

if [ "${TARGETS}" == "null" ]; then
	echo "Couldn't find any instance which fits the criteria."
	exit 1
fi

# Please refer to https://jmespath.org/examples.html for queries reference
ACTIVE=$(aws ssm list-command-invocations --filters "key=DocumentName,value=${DOCUMENT_NAME}" --query "CommandInvocations[?Status=='Pending' || Status=='InProgress']")

if [ "${ACTIVE}" != "[]" ]; then
	echo "Command is already active. Wait until the previous one is finished."
	echo "${ACTIVE}"
	exit 1
fi

COMMAND_ID=$(aws ssm send-command --document-name "${DOCUMENT_NAME}" --targets "[{\"Key\":\"InstanceIds\",\"Values\":${TARGETS}}]" | jq -r '.Command.CommandId')

while [[ ${CURRENT_STATUS} != "${FINAL_STATUS}" ]]; do
	CURRENT_STATUS=$(get_invocation_status "${COMMAND_ID}")
	echo "$(date '+%d/%m/%Y %H:%M:%S') :: Current status: ${CURRENT_STATUS}"

	if [[ ! " ${ACCEPTABLE_STATUSES[*]} " =~ ${CURRENT_STATUS} ]]; then
		if [[ ${CURRENT_STATUS} == "${FINAL_STATUS}" ]]; then break; fi
		echo "Command finished with a failure: ${CURRENT_STATUS}."
		exit 1
	fi

	sleep "${REFRESH_RATE}"
done

echo "Command finished successfully."
