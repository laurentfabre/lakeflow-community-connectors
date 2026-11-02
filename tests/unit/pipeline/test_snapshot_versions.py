"""Snapshot identity, retry and checkpoint contracts (no platform atomicity claim)."""

from unittest.mock import Mock

import pytest

from databricks.labs.community_connector.pipeline.snapshot_versions import versioned_snapshot_source

METADATA = {"snapshot_version": "opaque-z", "snapshot_fingerprint": "a" * 64}
TARGET = {"connection": "test", "destination": "raw", "primary_keys": ["id"], "scd_type": "1"}


def test_first_read_then_caught_up_and_restart():
    read = Mock(return_value=object())
    source = versioned_snapshot_source(METADATA, TARGET, read)
    frame, committed = source(None)
    assert frame is read.return_value
    assert source(committed) is None
    restarted = versioned_snapshot_source(dict(METADATA), dict(TARGET), read)
    assert restarted(committed) is None
    read.assert_called_once_with()


def test_retry_does_not_acknowledge_returned_frame():
    read = Mock(return_value=object())
    first = versioned_snapshot_source(METADATA, TARGET, read)(None)
    retry = versioned_snapshot_source(METADATA, TARGET, read)(None)
    assert retry == first
    assert read.call_count == 2


def test_load_failure_does_not_advance_progress():
    read = Mock(side_effect=[RuntimeError("injected extraction failure"), object()])
    source = versioned_snapshot_source(METADATA, TARGET, read)
    with pytest.raises(RuntimeError, match="injected extraction failure"):
        source(None)
    assert source(None)[1].startswith("00000000000000000001:")


def test_new_opaque_version_is_not_lexically_ordered():
    old = versioned_snapshot_source(METADATA, TARGET, Mock())(None)[1]
    new = versioned_snapshot_source({**METADATA, "snapshot_version": "opaque-a"}, TARGET, Mock())(
        old
    )[1]
    assert new > old
    assert new.startswith("00000000000000000002:")


@pytest.mark.parametrize(
    "change", ["fingerprint", "connection", "destination", "primary_keys", "scd_type"]
)
def test_same_version_changed_contract_is_read_again(change):
    old = versioned_snapshot_source(METADATA, TARGET, Mock())(None)[1]
    metadata, target = dict(METADATA), dict(TARGET)
    if change == "fingerprint":
        metadata["snapshot_fingerprint"] = "b" * 64
    else:
        target[change] = ["other"] if change == "primary_keys" else "other"
    read = Mock()
    source = versioned_snapshot_source(metadata, target, read)
    _, committed = source(old)
    assert committed > old
    assert source(committed) is None
    read.assert_called_once_with()


def test_empty_snapshot_is_still_delivered():
    empty_frame = []
    read = Mock(return_value=empty_frame)
    assert versioned_snapshot_source(METADATA, TARGET, read)(None)[0] is empty_frame


@pytest.mark.parametrize("checkpoint", [0, 1, True, "legacy", "0:a", "0" * 20 + ":" + "a" * 64])
def test_incompatible_checkpoint_fails_before_read(checkpoint):
    read = Mock()
    with pytest.raises(ValueError, match="Incompatible snapshot checkpoint"):
        versioned_snapshot_source(METADATA, TARGET, read)(checkpoint)
    read.assert_not_called()


@pytest.mark.parametrize(
    "metadata", [{}, {"snapshot_version": "v"}, {**METADATA, "snapshot_fingerprint": "bad"}]
)
def test_missing_capability_fails_before_read(metadata):
    read = Mock()
    with pytest.raises(ValueError, match="requires snapshot_"):
        versioned_snapshot_source(metadata, TARGET, read)
    read.assert_not_called()


def test_sequence_overflow_fails():
    with pytest.raises(ValueError, match="sequence exhausted"):
        versioned_snapshot_source(METADATA, TARGET, Mock())("9" * 20 + ":" + "f" * 64)
