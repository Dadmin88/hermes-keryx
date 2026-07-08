#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: migrate-to-keryx.sh [--dry-run] [--revert [BACKUP_PATH]] [--keryx-daemon PATH]

Migrates ~/.hermes/config.yaml from AgentAnycast settings to Keryx.

Options:
  --dry-run              Print planned changes without modifying files.
  --revert [BACKUP]      Restore config.yaml from a specific backup, the
                         recorded agency.keryx.migration_backup, or the latest
                         config.yaml.pre-keryx.*.bak file.
  --keryx-daemon PATH    Path to write into agency.daemon_bin. Defaults to
                         $HERMES_KERYX_DAEMON_BIN, keryxd on PATH, or
                         ~/.hermes/.keryx/bin/keryxd.
  -h, --help             Show this help.

Environment overrides:
  HERMES_HOME                    Hermes home directory (default: ~/.hermes)
  HERMES_CONFIG                  Config file path (default: $HERMES_HOME/config.yaml)
  HERMES_KERYX_DAEMON_ENDPOINT   Daemon endpoint (default: 127.0.0.1:50051)
USAGE
}

DRY_RUN=0
REVERT=0
REVERT_BACKUP=""
KERYX_DAEMON="${HERMES_KERYX_DAEMON_BIN:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --revert)
      REVERT=1
      shift
      if [[ $# -gt 0 && "$1" != --* ]]; then
        REVERT_BACKUP="$1"
        shift
      fi
      ;;
    --keryx-daemon)
      if [[ $# -lt 2 ]]; then
        echo "error: --keryx-daemon requires a path" >&2
        exit 2
      fi
      KERYX_DAEMON="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${KERYX_DAEMON}" ]]; then
  if command -v keryxd >/dev/null 2>&1; then
    KERYX_DAEMON="$(command -v keryxd)"
  else
    KERYX_DAEMON="${HERMES_HOME:-$HOME/.hermes}/.keryx/bin/keryxd"
  fi
fi

export DRY_RUN REVERT REVERT_BACKUP KERYX_DAEMON
export HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
export HERMES_CONFIG="${HERMES_CONFIG:-$HERMES_HOME/config.yaml}"
export HERMES_KERYX_DAEMON_ENDPOINT="${HERMES_KERYX_DAEMON_ENDPOINT:-127.0.0.1:50051}"

python3 - <<'PY'
from __future__ import annotations

import copy
import datetime as _dt
import glob
import os
import shutil
import stat
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover - depends on operator host
    raise SystemExit(
        "error: PyYAML is required to edit ~/.hermes/config.yaml. "
        "Install it in the Python used by this script."
    ) from exc

DRY_RUN = os.environ.get("DRY_RUN") == "1"
REVERT = os.environ.get("REVERT") == "1"
HERMES_HOME = Path(os.environ["HERMES_HOME"]).expanduser()
CONFIG_PATH = Path(os.environ["HERMES_CONFIG"]).expanduser()
KERYX_DAEMON = Path(os.environ["KERYX_DAEMON"]).expanduser()
DAEMON_ENDPOINT = os.environ["HERMES_KERYX_DAEMON_ENDPOINT"]
REVERT_BACKUP = os.environ.get("REVERT_BACKUP", "")

KERYX_DIR = HERMES_HOME / ".keryx"
ALLOWLIST_PATH = KERYX_DIR / "allowlist.toml"
RELAY_CONFIG_PATH = KERYX_DIR / "relay.toml"


def _now_stamp() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _unique_path(base: Path) -> Path:
    if not base.exists():
        return base
    stem = str(base)
    for idx in range(1, 1000):
        candidate = Path(f"{stem}.{idx}")
        if not candidate.exists():
            return candidate
    raise SystemExit(f"error: could not find unused backup path for {base}")


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"error: config file not found: {path}")
    raw = path.read_text(encoding="utf-8")
    loaded = yaml.safe_load(raw) if raw.strip() else {}
    if loaded is None:
        loaded = {}
    if not isinstance(loaded, dict):
        raise SystemExit(f"error: expected mapping at top level of {path}")
    return loaded


def _dump_yaml(data: dict[str, Any]) -> str:
    return yaml.safe_dump(data, sort_keys=False, default_flow_style=False)


def _ensure_map(parent: dict[str, Any], key: str) -> dict[str, Any]:
    value = parent.get(key)
    if value is None:
        value = {}
        parent[key] = value
    if not isinstance(value, dict):
        raise SystemExit(f"error: expected '{key}' to be a mapping in {CONFIG_PATH}")
    return value


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "allow", "allow_all"}
    return bool(value)


def _list_from(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, int)):
        return [str(value)]
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    return []


def _relay_security_sources(agency: dict[str, Any]) -> tuple[list[str], bool]:
    """Collect AgentAnycast relay peers from supported legacy config shapes."""
    peers: list[str] = []
    seen: set[str] = set()
    allow_all = False

    def add_many(items: list[str]) -> None:
        for item in items:
            clean = item.strip()
            if clean and clean not in seen:
                seen.add(clean)
                peers.append(clean)

    relay = agency.get("relay")
    if isinstance(relay, dict):
        add_many(_list_from(relay.get("allowlist")))
        allow_all = allow_all or _as_bool(relay.get("allow_all"))

    relay_security = agency.get("relay_security")
    if isinstance(relay_security, dict):
        add_many(_list_from(relay_security.get("allowlist")))
        allow_all = allow_all or _as_bool(relay_security.get("allow_all"))

    return peers, allow_all


def _toml_string(value: str) -> str:
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'


def _toml_array(values: list[str]) -> str:
    return "[" + ", ".join(_toml_string(v) for v in values) + "]"


def _allowlist_toml(peers: list[str]) -> str:
    lines = [
        "# Generated by scripts/migrate-to-keryx.sh from Hermes Agency relay allowlist.",
        "# Peers authorized to connect to the Keryx relay.",
        "",
    ]
    if not peers:
        lines.append("# No peers were present in agency.relay.allowlist.")
    for peer in peers:
        lines.extend(["[[allowed]]", f"peer_id = {_toml_string(peer)}", ""])
    return "\n".join(lines).rstrip() + "\n"


def _relay_toml(allow_all: bool) -> str:
    policy = "allow" if allow_all else "deny"
    lines = [
        "# Generated by scripts/migrate-to-keryx.sh.",
        "",
        "[relay]",
        'listen_addresses = ["/ip4/0.0.0.0/tcp/4001", "/ip4/0.0.0.0/udp/4001/quic-v1"]',
        "bootstrap_peers = []",
        "enable_mdns = false",
        "max_connections = 256",
        "max_reservations = 128",
        "max_reservations_per_peer = 4",
        "connection_timeout_ms = 30000",
        "use_ipv6 = false",
        "",
        "[security]",
        f"allowlist_path = {_toml_string(str(ALLOWLIST_PATH))}",
        f"empty_allowlist_policy = {_toml_string(policy)}",
        "",
        "[registry]",
        "ttl_seconds = 300",
        "max_skills_per_peer = 64",
    ]
    return "\n".join(lines) + "\n"


def _write_text_atomic(path: Path, text: str, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def _config_mode(path: Path) -> int:
    try:
        return stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError:
        return 0o600


def _print_plan(old: dict[str, Any], new: dict[str, Any], backup_path: Path, peers: list[str], allow_all: bool) -> None:
    old_agency = old.get("agency") if isinstance(old.get("agency"), dict) else {}
    old_keryx = old_agency.get("keryx") if isinstance(old_agency.get("keryx"), dict) else {}
    new_agency = new["agency"]
    new_keryx = new_agency["keryx"]
    policy = "allow" if allow_all else "deny"

    print("DRY RUN: no files changed" if DRY_RUN else "Applying Keryx migration")
    print(f"Config: {CONFIG_PATH}")
    print(f"Backup: {backup_path}")
    print("Planned config updates:")
    print(f"  agency.transport_backend: {old_agency.get('transport_backend')!r} -> {new_agency.get('transport_backend')!r}")
    print(f"  agency.daemon_bin: {old_agency.get('daemon_bin')!r} -> {new_agency.get('daemon_bin')!r}")
    print(f"  agency.keryx.daemon_endpoint: {old_keryx.get('daemon_endpoint')!r} -> {new_keryx.get('daemon_endpoint')!r}")
    print(f"  agency.keryx.allowlist_path: {old_keryx.get('allowlist_path')!r} -> {new_keryx.get('allowlist_path')!r}")
    print(f"  agency.keryx.relay_config_path: {old_keryx.get('relay_config_path')!r} -> {new_keryx.get('relay_config_path')!r}")
    print(f"  agency.keryx.migration_backup: {old_keryx.get('migration_backup')!r} -> {new_keryx.get('migration_backup')!r}")
    print("Generated Keryx relay files:")
    print(f"  {ALLOWLIST_PATH} ({len(peers)} peer(s))")
    print(f"  {RELAY_CONFIG_PATH} (empty_allowlist_policy={policy!r})")


def _migrate() -> None:
    config = _load_yaml(CONFIG_PATH)
    old_agency = config.get("agency") if isinstance(config.get("agency"), dict) else {}
    old_keryx = old_agency.get("keryx") if isinstance(old_agency.get("keryx"), dict) else {}
    existing_migration_backup = old_keryx.get("migration_backup")
    preserved_migration_backup: Path | None = None
    if isinstance(existing_migration_backup, str) and existing_migration_backup.strip():
        candidate = Path(existing_migration_backup).expanduser()
        if candidate.exists():
            preserved_migration_backup = candidate

    new_config = copy.deepcopy(config)
    agency = _ensure_map(new_config, "agency")
    peers, allow_all = _relay_security_sources(agency)
    backup_path = _unique_path(CONFIG_PATH.with_name(f"{CONFIG_PATH.name}.pre-keryx.{_now_stamp()}.bak"))
    migration_backup = preserved_migration_backup or backup_path

    agency["transport_backend"] = "keryx"
    agency["daemon_bin"] = str(KERYX_DAEMON)
    keryx = _ensure_map(agency, "keryx")
    keryx["daemon_endpoint"] = DAEMON_ENDPOINT
    keryx["allowlist_path"] = str(ALLOWLIST_PATH)
    keryx["relay_config_path"] = str(RELAY_CONFIG_PATH)
    keryx["migration_backup"] = str(migration_backup)

    allowlist_text = _allowlist_toml(peers)
    relay_text = _relay_toml(allow_all)
    config_text = _dump_yaml(new_config)

    _print_plan(config, new_config, backup_path, peers, allow_all)

    if DRY_RUN:
        return

    KERYX_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(KERYX_DIR, 0o700)
    shutil.copy2(CONFIG_PATH, backup_path)
    _write_text_atomic(ALLOWLIST_PATH, allowlist_text, 0o600)
    _write_text_atomic(RELAY_CONFIG_PATH, relay_text, 0o600)
    _write_text_atomic(CONFIG_PATH, config_text, _config_mode(CONFIG_PATH))
    print("Migration applied successfully.")


def _backup_from_config() -> Path | None:
    try:
        config = _load_yaml(CONFIG_PATH)
    except SystemExit:
        return None
    agency = config.get("agency") if isinstance(config.get("agency"), dict) else {}
    keryx = agency.get("keryx") if isinstance(agency.get("keryx"), dict) else {}
    backup = keryx.get("migration_backup")
    if isinstance(backup, str) and backup.strip():
        path = Path(backup).expanduser()
        if path.exists():
            return path
    return None


def _latest_pre_keryx_backup() -> Path | None:
    pattern = str(CONFIG_PATH.with_name(f"{CONFIG_PATH.name}.pre-keryx.*.bak*"))
    candidates = [Path(p) for p in glob.glob(pattern)]
    if not candidates:
        return None
    candidates.sort(key=lambda p: (p.stat().st_mtime, str(p)))
    return candidates[-1]


def _revert() -> None:
    if REVERT_BACKUP:
        backup = Path(REVERT_BACKUP).expanduser()
    else:
        backup = _backup_from_config() or _latest_pre_keryx_backup()
        if backup is None:
            raise SystemExit(
                f"error: no migration backup recorded and no {CONFIG_PATH.name}.pre-keryx.*.bak file found"
            )

    if not backup.exists():
        raise SystemExit(f"error: backup not found: {backup}")

    pre_revert = _unique_path(CONFIG_PATH.with_name(f"{CONFIG_PATH.name}.pre-revert.{_now_stamp()}.bak"))
    print("DRY RUN: no files changed" if DRY_RUN else "Reverting Keryx migration")
    print(f"Restore {CONFIG_PATH} from {backup}")
    if CONFIG_PATH.exists():
        print(f"Current config backup before revert: {pre_revert}")

    if DRY_RUN:
        return

    if CONFIG_PATH.exists():
        shutil.copy2(CONFIG_PATH, pre_revert)
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(backup, CONFIG_PATH)
    print("Revert applied successfully.")


if REVERT:
    _revert()
else:
    _migrate()
PY
