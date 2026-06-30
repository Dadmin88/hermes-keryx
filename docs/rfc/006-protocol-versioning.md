# RFC 006: Protocol Versioning

## Package

The v1 protobuf package is `hermes.keryx.v1`.

## Compatibility rules

- Additive fields are preferred.
- Field numbers are never reused.
- Removed fields must be reserved.
- Clients and daemons report protocol and implementation versions in status/doctor surfaces.
- Incompatible daemon/client or relay/node combinations must fail explicitly with typed version errors.

## Breaking-change checks

`buf.yaml` defines lint and breaking-change policy. CI should run buf lint/breaking once buf tooling is installed in the environment.
