"""Checkpointed snapshot selection without application-owned completion state.

Lakeflow supplies the last processed sequence to the callback and commits its
progress with the target. Returning a DataFrame never acknowledges success here.
"""

import hashlib
import json
import re

_SEQUENCE = re.compile(r"([0-9]{20}):([0-9a-f]{64})\Z")


def versioned_snapshot_source(metadata: dict, target_identity: dict, read_snapshot):
    """Build a Lakeflow snapshot callback for one pinned version per update.

    Snapshot IDs are opaque: equality determines whether to read them. A local
    monotonic ordinal supplies Lakeflow's ordering; the suffix carries identity
    through the durable checkpoint. Never substitute empty rows for no change.
    """
    version = metadata.get("snapshot_version")
    fingerprint = metadata.get("snapshot_fingerprint")
    if not isinstance(version, str) or not version:
        raise ValueError("skip_unchanged_versions requires snapshot_version metadata")
    if not isinstance(fingerprint, str) or not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
        raise ValueError("skip_unchanged_versions requires snapshot_fingerprint metadata")
    identity = hashlib.sha256(
        json.dumps(
            {"version": version, "fingerprint": fingerprint, "target": target_identity},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    def next_snapshot(latest_snapshot_version):
        ordinal = 0
        if latest_snapshot_version is not None:
            match = (
                _SEQUENCE.fullmatch(latest_snapshot_version)
                if isinstance(latest_snapshot_version, str)
                else None
            )
            if match is None or int(match[1]) == 0:
                raise ValueError(
                    "Incompatible snapshot checkpoint: use a new destination or explicitly "
                    "migrate its checkpoint before enabling skip_unchanged_versions"
                )
            ordinal = int(match[1])
            if match[2] == identity:
                return None
        if ordinal >= 10**20 - 1:
            raise ValueError("Snapshot sequence exhausted; explicitly migrate the checkpoint")
        # Errors from load or extraction must propagate. Only Lakeflow may commit
        # this sequence after it has processed the complete DataFrame.
        return read_snapshot(), f"{ordinal + 1:020d}:{identity}"

    return next_snapshot
