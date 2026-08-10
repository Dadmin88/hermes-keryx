#!/usr/bin/env python3
from pathlib import Path

path = Path("crates/keryx-daemon/src/routing.rs")
text = path.read_text(encoding="utf-8")

test = r'''
    #[test]
    fn relay_target_metadata_is_canonicalized() {
        let target = PeerId::new("node-canonical-target").unwrap();
        let mut envelope = TaskEnvelope::default();
        for key in RELAY_TARGET_METADATA_KEYS {
            envelope
                .metadata
                .insert((*key).to_string(), "node-poisoned".to_string());
        }
        canonicalize_relay_target_metadata(&mut envelope, &target);
        for key in RELAY_TARGET_METADATA_KEYS {
            if *key != CANONICAL_RELAY_TARGET_METADATA_KEY {
                assert!(!envelope.metadata.contains_key(*key));
            }
        }
        assert_eq!(
            envelope
                .metadata
                .get(CANONICAL_RELAY_TARGET_METADATA_KEY)
                .map(String::as_str),
            Some(target.as_str())
        );
    }
'''

if text.count(test) != 1:
    raise SystemExit(f"expected exactly one generated routing regression, found {text.count(test)}")
text = text.replace(test, "", 1)
marker = "mod tests {\n    use super::*;\n    use keryx_proto::v1::TaskId as ProtoTaskId;\n"
if text.count(marker) != 1:
    raise SystemExit("routing test-module anchor drifted")
text = text.replace(marker, marker + test, 1)
path.write_text(text, encoding="utf-8")
print("generated routing regression anchored in mod tests")
