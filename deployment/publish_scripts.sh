#!/usr/bin/env bash

DOCUMENT_NAME="Deploy-dqe"
REGIONS="eu-west-1 us-east-1 sa-east-1 ap-northeast-1"
COMMAND_TOGGLES="--content file://ssm_deployment.json --name $DOCUMENT_NAME --document-format JSON --target-type /AWS::EC2::Instance"
TAG_LIST="Key=Owner,Value=RGM Key=CostCenter,Value=RGM Key=ServiceName,Value=DQE Key=ProvisioningMethod,Value=Manual"

for REGION in $REGIONS; do
	echo $REGION
	EXISTING_DOCUMENT=$(aws ssm describe-document --region "$REGION" --name "$DOCUMENT_NAME")
	if [ -z "$EXISTING_DOCUMENT" ]; then
		aws ssm create-document --region "$REGION" $COMMAND_TOGGLES --document-type Command --tags $TAG_LIST || true
	else
		aws ssm update-document --region "$REGION" $COMMAND_TOGGLES --document-version \$LATEST || true
	fi
done