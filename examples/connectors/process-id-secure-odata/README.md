# Secure process-by-ID OData connector

This Rust sidecar adapts one workbook from an OData service to the parameter
artifact expected by
[`process-id-secure.yaml`](../../blueprints/process-id-secure.yaml). It accepts
only resources shaped like `/workbooks/<integer-id>/groups/daily`, queries the
matching OData workbook, and returns its one `daily` parameter set.

The example deliberately does not return `bwp_user` or `bwp_password`. The
secure blueprint resolves those values on the selected agent from `BWP_USER`
and the `bwp-password` secret file.

## Assumed OData contract

The example calls:

```text
GET <ODATA_BASE_URL>/Workbooks(<id>)
  ?$select=Id,WorkbookName
  &$expand=ParameterSets($filter=Group eq 'daily';$select=...)
```

The response is one workbook entity with `Id`, `WorkbookName`, and a
`ParameterSets` navigation property. See
[`odata-workbook.example.json`](odata-workbook.example.json) for a complete
example that also contains a group the connector ignores. Rename the Rust
response fields and the entity/navigation names if the real service uses a
different OData model.

The connector requires exactly one returned parameter set whose `Group` is
`daily`. No daily set returns `404`; duplicate daily sets, an ID mismatch, or a
malformed upstream document returns `502`.

## Run

Set separate tokens for the coordinator-to-connector and connector-to-OData
hops:

```sh
export PROCESS_ID_CONNECTOR_TOKEN=development-connector-token
export ODATA_BASE_URL=https://odata.example.com/v1/
export ODATA_BEARER_TOKEN=development-odata-token
cargo run -p process-id-secure-odata-connector
```

`CONNECTOR_LISTEN_ADDR` defaults to `127.0.0.1:9010` and
`ODATA_TIMEOUT_SECONDS` defaults to `10`. The upstream URL must use HTTPS;
plain HTTP is accepted only for a loopback development service. Redirects are
disabled so the OData bearer token cannot be forwarded to another origin.
Upstream responses must be JSON and are capped at 1 MiB.

Point `SCHEDULER_CONNECTOR_CONFIG` at
[`connector.example.yaml`](connector.example.yaml), export the same
`PROCESS_ID_CONNECTOR_TOKEN` in the coordinator environment, and create the
copyable
[`process-id-secure-odata.example.yaml`](../../schedules/process-id-secure-odata.example.yaml)
schedule after replacing its absolute blueprint path.

You can verify the connector contract directly:

```sh
curl -i http://127.0.0.1:9010/v1/artifacts/fetch \
  -H 'Authorization: Bearer development-connector-token' \
  -H 'Content-Type: application/json' \
  -d '{"api_version":"scheduler.connector/v1","kind":"parameters","resource":"/workbooks/2147483647/groups/daily"}'
```

This is a focused integration example, not a drop-in production service. Add
your deployment's TLS termination, token rotation, structured audit events,
and upstream availability monitoring.
