use crate::*;
use cmd_lib::*;
use comfy_table::{Table, presets};
use std::collections::HashMap;

pub fn run_cmd_tool(tool_kind: ToolKind) -> CmdResult {
    match tool_kind {
        ToolKind::GenUuids { num, file } => {
            xtask_tools::gen_uuids(num, &file)?;
        }
        ToolKind::DescribeStack { stack_name } => {
            describe_stack(&stack_name)?;
        }
    }
    Ok(())
}

fn describe_stack(stack_name: &str) -> CmdResult {
    println!("=== All EC2 Instances in Stack: {} ===", stack_name);

    // Get direct EC2 instance IDs from the CloudFormation stack
    let direct_instance_ids = run_fun! {
        aws cloudformation describe-stack-resources
            --stack-name "$stack_name"
            --query r#"StackResources[?ResourceType==`AWS::EC2::Instance`].PhysicalResourceId"#
            --output text
    }?;

    // Get Auto Scaling Group names from the CloudFormation stack
    let asg_names = run_fun! {
        aws cloudformation describe-stack-resources
            --stack-name "$stack_name"
            --query r#"StackResources[?ResourceType==`AWS::AutoScaling::AutoScalingGroup`].PhysicalResourceId"#
            --output text
    }?;

    // Collect all ASG instance IDs
    let mut asg_instance_ids = Vec::new();
    if !asg_names.trim().is_empty() {
        for asg_name in asg_names.split_whitespace() {
            let asg_instances = run_fun! {
                aws autoscaling describe-auto-scaling-groups
                    --auto-scaling-group-names "$asg_name"
                    --query r#"AutoScalingGroups[].Instances[].InstanceId"#
                    --output text
            }?;

            if !asg_instances.trim().is_empty() {
                asg_instance_ids.extend(asg_instances.split_whitespace().map(|s| s.to_string()));
            }
        }
    }

    // Get instances by CloudFormation stack tag
    let tagged_instance_ids = run_fun! {
        aws ec2 describe-instances
            --filters "Name=tag:aws:cloudformation:stack-name,Values=$stack_name"
                      "Name=instance-state-name,Values=pending,running,stopping,stopped"
            --query r#"Reservations[].Instances[].InstanceId"#
            --output text
    }?;

    // Get instances by Name tag prefix
    let name_prefix_instance_ids = run_fun! {
        aws ec2 describe-instances
            --filters "Name=tag:Name,Values=${stack_name}/*"
                      "Name=instance-state-name,Values=pending,running,stopping,stopped"
            --query r#"Reservations[].Instances[].InstanceId"#
            --output text
    }?;

    // Combine all instance IDs and remove duplicates
    let mut all_instance_ids = Vec::new();

    // Add direct instance IDs
    all_instance_ids.extend(
        direct_instance_ids
            .split_whitespace()
            .map(|s| s.to_string()),
    );

    // Add ASG instance IDs
    all_instance_ids.extend(asg_instance_ids);

    // Add tagged instance IDs
    all_instance_ids.extend(
        tagged_instance_ids
            .split_whitespace()
            .map(|s| s.to_string()),
    );

    // Add name prefix instance IDs
    all_instance_ids.extend(
        name_prefix_instance_ids
            .split_whitespace()
            .map(|s| s.to_string()),
    );

    // Remove duplicates and empty strings
    all_instance_ids.sort();
    all_instance_ids.dedup();
    all_instance_ids.retain(|id| !id.is_empty());

    if all_instance_ids.is_empty() {
        warn!("No EC2 instances found in stack: {}", stack_name);
        return Ok(());
    }

    // Get zone name to zone ID mapping
    let zone_info = run_fun! {
        aws ec2 describe-availability-zones
            --query r#"AvailabilityZones[].[ZoneName,ZoneId]"#
            --output text
    }?;

    let mut zone_map = HashMap::new();
    for line in zone_info.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() == 2 {
            zone_map.insert(parts[0].to_string(), parts[1].to_string());
        }
    }

    // Get instance details
    let instance_details = run_fun! {
        aws ec2 describe-instances
            --instance-ids $[all_instance_ids]
            --query r#"Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,InstanceId,State.Name,InstanceType,Placement.AvailabilityZone,PrivateIpAddress]"#
            --output text
    }?;

    // Create and populate the table
    let mut table = Table::new();
    table.load_preset(presets::ASCII_BORDERS_ONLY_CONDENSED);
    table.set_header(vec![
        "Name",
        "InstanceId",
        "State",
        "InstanceType",
        "AvailabilityZone",
        "ZoneId",
        "PrivateIP",
    ]);

    for line in instance_details.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 6 {
            let name = if parts[0] == "None" { "" } else { parts[0] };
            let instance_id = parts[1];
            let state = parts[2];
            let instance_type = parts[3];
            let az = parts[4];
            let private_ip = if parts[5] == "None" { "-" } else { parts[5] };
            let zone_id = zone_map.get(az).map(|s| s.as_str()).unwrap_or("N/A");

            table.add_row(vec![
                name,
                instance_id,
                state,
                instance_type,
                az,
                zone_id,
                private_ip,
            ]);
        }
    }

    println!("{table}");
    Ok(())
}
