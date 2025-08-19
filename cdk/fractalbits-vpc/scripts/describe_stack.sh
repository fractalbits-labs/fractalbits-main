#!/bin/bash

STACK_NAME=${1:-FractalbitsVpcStack}

# Get direct EC2 instance IDs from the CloudFormation stack
DIRECT_INSTANCE_IDS=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" --query 'StackResources[?ResourceType==`AWS::EC2::Instance`].PhysicalResourceId' --output text)

# Get Auto Scaling Group names from the CloudFormation stack
ASG_NAMES=$(aws cloudformation describe-stack-resources --stack-name "$STACK_NAME" --query 'StackResources[?ResourceType==`AWS::AutoScaling::AutoScalingGroup`].PhysicalResourceId' --output text)

# Collect all ASG instance IDs
ASG_INSTANCE_IDS=""
if [ -n "$ASG_NAMES" ]; then
    for ASG_NAME in $ASG_NAMES; do
        ASG_INSTANCES=$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$ASG_NAME" --query 'AutoScalingGroups[].Instances[].InstanceId' --output text)
        if [ -n "$ASG_INSTANCES" ]; then
            ASG_INSTANCE_IDS="$ASG_INSTANCE_IDS $ASG_INSTANCES"
        fi
    done
fi

# Combine all instance IDs
ALL_INSTANCE_IDS="$DIRECT_INSTANCE_IDS $ASG_INSTANCE_IDS"

# Display all instances in a single table
if [ -n "$ALL_INSTANCE_IDS" ]; then
    echo "=== All EC2 Instances in Stack: $STACK_NAME ==="
    aws ec2 describe-instances --instance-ids $ALL_INSTANCE_IDS --query 'Reservations[].Instances[].{InstanceId:InstanceId,Name:Tags[?Key==`Name`]|[0].Value,InstanceType:InstanceType,AvailabilityZone:Placement.AvailabilityZone,State:State.Name}' --output table
else
    echo "No EC2 instances found in stack: $STACK_NAME"
fi
