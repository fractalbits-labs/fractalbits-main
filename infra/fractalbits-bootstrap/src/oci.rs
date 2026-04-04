use crate::common::{BIN_PATH, DDB_SERVICE_DISCOVERY_TABLE, ETC_PATH};
use crate::config::BootstrapConfig;
use cmd_lib::*;
use std::io::Error;
use std::time::{Duration, Instant};

/// Write a row to OCI NoSQL via the OCI CLI.
/// Used during bootstrap to initialize service discovery state before RSS starts.
pub fn oci_nosql_put_row(config: &BootstrapConfig, table_name: &str, row_json: &str) -> CmdResult {
    let oci = config
        .oci
        .as_ref()
        .ok_or_else(|| Error::other("OCI config missing from bootstrap config for NoSQL write"))?;
    let compartment = &oci.compartment_ocid;

    run_cmd!(
        oci nosql row update
            --auth instance_principal
            --table-name-or-id $table_name
            --compartment-id $compartment
            --value $row_json
            --force
    )
}

pub(crate) fn create_oci_nosql_register_and_deregister_service(
    config: &BootstrapConfig,
    service_id: &str,
) -> CmdResult {
    create_oci_nosql_register_service(config, service_id)?;
    create_oci_nosql_deregister_service(config, service_id)?;
    Ok(())
}

fn create_oci_nosql_register_service(config: &BootstrapConfig, service_id: &str) -> CmdResult {
    let oci = config.oci.as_ref().ok_or_else(|| {
        Error::other("OCI config missing from bootstrap config for NoSQL registration")
    })?;
    let compartment = &oci.compartment_ocid;
    let register_script = format!("{BIN_PATH}oci-nosql-register.sh");
    let table_name = format!("{DDB_SERVICE_DISCOVERY_TABLE}-{service_id}");

    let systemd_unit_content = format!(
        r##"[Unit]
Description=OCI NoSQL Service Registration
After=network-online.target

[Service]
Type=oneshot
ExecStart={register_script}

[Install]
WantedBy=multi-user.target
"##
    );

    let auth_header = "Authorization: Bearer Oracle";
    let register_script_content = format!(
        r###"#!/bin/bash
set -e
service_id={service_id}
compartment="{compartment}"
table_name="{table_name}"
instance_id=$(curl -sf -H "{auth_header}" http://169.254.169.254/opc/v2/instance/displayName)
private_ip=$(curl -sf -H "{auth_header}" http://169.254.169.254/opc/v2/vnics/0/privateIp)

echo "Registering itself ($instance_id,$private_ip) to OCI NoSQL with service_id $service_id" >&2

MAX_RETRIES=30
retry_count=0
success=false

while [ $retry_count -lt $MAX_RETRIES ] && [ "$success" = "false" ]; do
    retry_count=$((retry_count + 1))

    row_json=$(printf '{{"id":"%s","value":"%s","version":1}}' "$instance_id" "$private_ip")

    if oci nosql row update \
        --auth instance_principal \
        --table-name-or-id "$table_name" \
        --compartment-id "$compartment" \
        --value "$row_json" \
        --force >/dev/null 2>&1; then
        echo "Registered service on attempt $retry_count" >&2
        success=true
    else
        echo "Registration failed on attempt $retry_count, retrying..." >&2
        sleep 2
    fi
done

if [ "$success" = "false" ]; then
    echo "FATAL: Failed to register service $service_id after $MAX_RETRIES attempts" >&2
    exit 1
fi

echo "Done" >&2
"###
    );

    run_cmd! {
        echo $register_script_content > $register_script;
        chmod +x $register_script;

        echo $systemd_unit_content > ${ETC_PATH}oci-nosql-register.service;
        systemctl enable --now ${ETC_PATH}oci-nosql-register.service;
    }?;
    Ok(())
}

fn create_oci_nosql_deregister_service(config: &BootstrapConfig, service_id: &str) -> CmdResult {
    let oci = config.oci.as_ref().ok_or_else(|| {
        Error::other("OCI config missing from bootstrap config for NoSQL deregistration")
    })?;
    let compartment = &oci.compartment_ocid;
    let deregister_script = format!("{BIN_PATH}oci-nosql-deregister.sh");
    let table_name = format!("{DDB_SERVICE_DISCOVERY_TABLE}-{service_id}");

    let auth_header = "Authorization: Bearer Oracle";
    let systemd_unit_content = format!(
        r##"[Unit]
Description=OCI NoSQL Service Deregistration
After=network-online.target
Before=reboot.target halt.target poweroff.target kexec.target

DefaultDependencies=no

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart={deregister_script}

[Install]
WantedBy=reboot.target halt.target poweroff.target kexec.target
"##
    );

    let deregister_script_content = format!(
        r###"#!/bin/bash
set -e
service_id={service_id}
compartment="{compartment}"
table_name="{table_name}"
instance_id=$(curl -sf -H "{auth_header}" http://169.254.169.254/opc/v2/instance/displayName)

echo "Deregistering itself ($instance_id) from OCI NoSQL with service_id $service_id" >&2

key_json=$(printf '[{{"id":"%s"}}]' "$instance_id")

oci nosql row delete \
    --auth instance_principal \
    --table-name-or-id "$table_name" \
    --compartment-id "$compartment" \
    --key "$key_json" \
    --force 2>/dev/null || true

echo "Done" >&2
"###
    );

    run_cmd! {
        echo $deregister_script_content > $deregister_script;
        chmod +x $deregister_script;

        echo $systemd_unit_content > ${ETC_PATH}oci-nosql-deregister.service;
        systemctl enable ${ETC_PATH}oci-nosql-deregister.service;
    }?;
    Ok(())
}

pub(crate) fn get_service_ips_oci_nosql(
    config: &BootstrapConfig,
    service_id: &str,
    expected_min_count: usize,
) -> Vec<String> {
    info!("Waiting for {expected_min_count} {service_id} service(s) via OCI NoSQL");
    let oci = config
        .oci
        .as_ref()
        .expect("OCI config required for NoSQL service discovery");
    let compartment = &oci.compartment_ocid;
    let table_name = format!("{DDB_SERVICE_DISCOVERY_TABLE}-{service_id}");

    let start_time = Instant::now();
    let timeout = Duration::from_secs(600);

    loop {
        if start_time.elapsed() > timeout {
            cmd_die!("Timeout waiting for ${service_id} service(s) via OCI NoSQL");
        }

        let stmt = format!("SELECT * FROM \"{}\"", table_name);

        let res = run_fun!(
            oci nosql query execute
                --auth instance_principal
                --compartment-id $compartment
                --statement $stmt
                2>/dev/null
        );

        if let Ok(output) = res
            && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&output)
        {
            let ips: Vec<String> = parsed["data"]["items"]
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item["value"].as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            if ips.len() >= expected_min_count {
                info!("Found a list of {service_id} services via OCI NoSQL: {ips:?}");
                return ips;
            }
            info!(
                "Found {} of {} {service_id} services, waiting...",
                ips.len(),
                expected_min_count
            );
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}
