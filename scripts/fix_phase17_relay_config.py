#!/usr/bin/env python3
"""Repair relay TOML endpoint defaults and runtime projection."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "crates/keryx-relay/src/security.rs"
text = path.read_text()
wrong_default = '''            use_ipv6: false,
            health_grpc_bind: self.relay.health_grpc_bind.clone(),
            health_http_bind: self.relay.health_http_bind.clone(),
            registry_grpc_bind: self.relay.registry_grpc_bind.clone(),
'''
correct_default = '''            use_ipv6: false,
            health_grpc_bind: crate::config::default_health_grpc_bind(),
            health_http_bind: crate::config::default_health_http_bind(),
            registry_grpc_bind: crate::config::default_registry_grpc_bind(),
'''
if wrong_default in text:
    text = text.replace(wrong_default, correct_default, 1)

wrong_projection = '''            use_ipv6: self.relay.use_ipv6,
            health_grpc_bind: crate::config::default_health_grpc_bind(),
            health_http_bind: crate::config::default_health_http_bind(),
            registry_grpc_bind: crate::config::default_registry_grpc_bind(),
'''
correct_projection = '''            use_ipv6: self.relay.use_ipv6,
            health_grpc_bind: self.relay.health_grpc_bind.clone(),
            health_http_bind: self.relay.health_http_bind.clone(),
            registry_grpc_bind: self.relay.registry_grpc_bind.clone(),
'''
if wrong_projection in text:
    text = text.replace(wrong_projection, correct_projection, 1)
path.write_text(text)
