#!/usr/bin/env python3
"""Unit rows for the shared lab's seed topics in `scripts/test-k8s-scram.py`.

    python3 scripts/test_k8s_scram_seed.py      (or: python3 -m pytest scripts/test_k8s_scram_seed.py)

No cluster, no network: the rows read the broker script the installer's `setup`
phase pipes into `kafka-source`, as `seed_topic_script` renders it.

LAB-SEED-TOPIC-RETENTION: the seed topics were created with the broker's
default 7-day retention, so `orders`/`payments` emptied a week after the lab was
built and every Backup of them captured nothing. The topic-creation command must
carry `retention.ms=-1`; the old command (without it) must be REFUSED by the
same check, or the check is not a check.
"""

from __future__ import annotations

import atexit
import importlib.util
import os
import pathlib
import shlex
import shutil
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
# Importing the installer creates its state directory; keep that out of /tmp/logweir-scram-e2e.
os.environ["LOGWEIR_SCRAM_OUT"] = tempfile.mkdtemp(prefix="scram-seed-unit-")
atexit.register(shutil.rmtree, os.environ["LOGWEIR_SCRAM_OUT"], True)
_spec = importlib.util.spec_from_file_location("test_k8s_scram", HERE / "test-k8s-scram.py")
scram = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(scram)

FAILURES: list[str] = []


def row(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f" — {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def topic_create_argv(script: str) -> list[str]:
    """The one `kafka-topics.sh --create` command in a broker script, as argv."""
    lines = [line for line in script.splitlines() if "kafka-topics.sh" in line]
    assert len(lines) == 1, lines
    return shlex.split(lines[0])


def configs(argv: list[str]) -> dict[str, str]:
    out = {}
    for flag, value in zip(argv, argv[1:]):
        if flag == "--config":
            key, _, val = value.partition("=")
            out[key] = val
    return out


def never_expires(script: str) -> bool:
    argv = topic_create_argv(script)
    return "--create" in argv and configs(argv).get("retention.ms") == "-1"


def the_pre_fix_script(topic: str) -> str:
    """What `setup` rendered before the fix (scripts/test-k8s-scram.py@ac00819:310)."""
    return (
        f"/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic {topic}"
        " --partitions 1 --replication-factor 1\n"
    )


def test_seed_topics_never_expire() -> None:
    for topic in scram.SEED_TOPICS:
        script = scram.seed_topic_script(topic)
        argv = topic_create_argv(script)
        assert argv[argv.index("--topic") + 1] == topic
        assert never_expires(script), script


def test_pre_fix_command_is_refused() -> None:
    # The negative control: the command the lab was built with must fail the check.
    for topic in scram.SEED_TOPICS:
        assert not never_expires(the_pre_fix_script(topic))


def test_seed_is_otherwise_unchanged() -> None:
    # Same topics, one partition, replication 1, the same 100 records per topic.
    assert scram.SEED_TOPICS == ("orders", "payments")
    for topic in scram.SEED_TOPICS:
        script = scram.seed_topic_script(topic)
        argv = topic_create_argv(script)
        assert argv[argv.index("--partitions") + 1] == "1"
        assert argv[argv.index("--replication-factor") + 1] == "1"
        assert list(configs(argv)) == ["retention.ms"]
        producer = [line for line in script.splitlines() if "kafka-console-producer.sh" in line]
        assert len(producer) == 1
        records = [f"{topic}-record-{n:03}" for n in range(1, 101)]
        assert shlex.split(producer[0].split(" | ")[0])[2:] == records
        assert producer[0].endswith(f"--topic {topic}")


def main() -> int:
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                row(name, True)
            except AssertionError as exc:
                row(name, False, repr(exc))
    print(f"{len(FAILURES)} failed")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
