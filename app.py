#!/usr/bin/env python3

import aws_cdk as cdk
from Constructs.state_machine_stack import StateMachineStack

# Variables
aws_acccount = "AWS_ACCOUNT_ID"
region = "AWS_REGION"

app = cdk.App()

StateMachineStack(
    app,
    "AWSStateFunctionPartialRuns",
    env=cdk.Environment(account=aws_acccount, region=region),
    tags={"Project": "Step Function partial runs"}
)

app.synth()
