"""Run the nine assignment experiments and export machine-readable evidence."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import csv
import json
from pathlib import Path
import random
import statistics
import tempfile
from threading import Barrier, Event
import time
from unittest.mock import patch

from kvstore import Cache, Cluster, Conflict, ReplyLost, Unavailable


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def expect(error, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except error:
        return
    raise AssertionError(f"Expected {error.__name__}: {fn.__name__}")


def replica_crash(c):
    c.put("flag", "off", "before")
    c.stop("B")
    result = c.put("flag", "on", "after")
    require(c.get("flag")["value"] == "on", "A and C must still serve")
    require(c.nodes["C"].call("last") == result["revision"], "C must persist write")
    c.start("B")
    c.sync("B")
    require(c.nodes["B"].call("log", 1, 2) == c.nodes["A"].call("log", 1, 2),
            "Recovered B must have the same log")
    return dict(value="on", committed=result["revision"], recovered_B=2)


def partition_duplicates(c):
    first = c.put("flag", "off", "init")
    c.sync("C")
    sent_before = c.replication_rpcs
    c.partition("C", True)
    require(not c.can_talk("A", "C") and not c.can_talk("C", "A"), "not bidirectional")
    c.duplicates = 3
    operation = ("flag", "on", "repeat", first["revision"])
    results = [c.put(*operation) for _ in range(4)]
    require(results.count(results[0]) == 4 and results[0]["ok"], "dedupe failed")
    require(c.nodes["B"].call("last") == 2 and c.nodes["C"].call("last") == 1,
            "duplicate replication or isolated replica changed log")
    require(c.replication_rpcs - sent_before == 4, "expected original delivery and three peer replays")
    expect(Conflict, c.put, "flag", "different", "repeat", first["revision"])
    c.partition("A", True)
    expect(Unavailable, c.put, "flag", "bad", "denied")
    expect(Unavailable, c.get, "flag")
    c.partition("A", False)
    c.partition("C", False)
    c.sync("C")
    require(c.nodes["C"].call("last") == 2 and c.get("flag")["value"] == "on",
            "partition healing should preserve exactly one change")
    return dict(client_replays=3, peer_replays=3, revision=2, healed_C=2, isolated_A="rejected")


def delayed_message(c):
    c.put("flag", "old", "old")
    c.sync("C")
    cache = Cache(c, "flag", ttl=0.5)
    cache.get()
    old_packet = c.nodes["A"].call("log", 1, 1)
    admitted = Event()

    def delayed_delivery():
        started = time.monotonic()
        admitted.set()
        time.sleep(10)
        c.deliver("C", old_packet)
        return time.monotonic() - started

    with ThreadPoolExecutor(max_workers=1) as pool:
        late = pool.submit(delayed_delivery)
        require(admitted.wait(5), "old packet was not queued")
        update = c.put("flag", "new", "new")
        c.sync("C")
        _, events = c.watch("flag", 0)
        require(cache.apply(events[-1]) and not late.done(), "the new packet must arrive first")
        seconds = late.result()
    accepted_late_event = cache.apply(events[0])
    require(seconds >= 10 and not accepted_late_event, "stale event applied")
    require(cache.value == "new" and c.nodes["C"].call("log", 1, 2)[-1][0] == 2,
            "reordered message regressed replica or cache")
    require(c.get("flag")["revision"] == update["revision"], "strong read regressed")
    return dict(delay_seconds=round(seconds, 3), received_revisions=[2, 1], value=cache.value)


def memory_loss(c):
    initial = c.put("flag", "on", "persist")
    old_pid = c.nodes["A"].pid
    c.stop("A")
    expect(Unavailable, c.get, "flag")
    c.start("A")
    new_pid = c.nodes["A"].pid
    require(new_pid != old_pid, "expected a new process")
    require(c.get("flag")["value"] == "on", "committed KV lost")
    require(c.put("flag", "on", "persist") == initial, "dedupe state lost")
    followup = c.put("flag", "new", "after-reboot", initial["revision"])
    require(followup["ok"] and followup["revision"] == 2, "revision did not resume")
    return dict(old_pid=old_pid, new_pid=new_pid, recovered=initial["revision"], next_revision=2)


def learner_crash(c):
    for i in range(6):
        c.put(f"cfg/{i}", str(i), f"base-{i}")
    c.add_learner("D")
    target = c.watermark()
    c.migrate("D", target, limit=3)
    partial = c.nodes["D"].call("last")
    require(partial == 3 and not c.learner_ready("D"), "partial copy was marked ready")
    c.stop("D")
    c.put("cfg/live", "1", "during-copy")
    c.start("D")
    resumed = c.nodes["D"].call("last")
    require(resumed == partial, "migration cursor lost")
    c.migrate("D", target)
    at_snapshot = c.nodes["D"].call("last")
    require(at_snapshot == 6 and not c.learner_ready("D"), "tail update missing")
    c.migrate("D", c.watermark())
    end = c.nodes["D"].call("last")
    require(c.learner_ready("D") and end == 7, "learner did not finish")
    require(c.nodes["D"].call("log", 1, end) == c.nodes["A"].call("log", 1, end),
            "learner data mismatch")
    c.stop("B")
    c.stop("C")
    expect(Unavailable, c.get, "cfg/live")
    return dict(copied_before_crash=partial, baseline=target, resumed=resumed, final=end,
                learner_counts_for_quorum=False)


def hotspot(c, hot_percent=60):
    c.put("cfg/hot", "on", "init-hot")
    for i in range(10):
        c.put(f"cfg/{i}", "stable", f"init-{i}")
    count = 1000
    hot_count = count * hot_percent // 100
    keys = ["cfg/hot"] * hot_count + [f"cfg/{i % 10}" for i in range(count - hot_count)]
    random.Random(42).shuffle(keys)
    cache = Cache(c, "cfg/hot")

    def measure(read):
        before = c.strong_gets
        timings = []
        for key in keys:
            started = time.perf_counter()
            result = read(key)
            timings.append((time.perf_counter() - started) * 1e6)
            require(result == ("on" if key == "cfg/hot" else "stable"), "wrong hot/cold result")
        return dict(strong_gets=c.strong_gets - before,
                    p50_us=round(statistics.median(timings), 1),
                    p95_us=round(sorted(timings)[949], 1))

    baseline = measure(lambda key: c.get(key)["value"])
    cached = measure(lambda key: cache.get()["value"] if key == cache.key else c.get(key)["value"])
    stop = Event()
    watch = cache.follow(stop, interval=0.05)
    with ThreadPoolExecutor(max_workers=1) as pool:
        watching = pool.submit(next, watch)
        try:
            started = time.monotonic()
            put = c.put("cfg/hot", "off", "change")
            delivered = watching.result(timeout=3)
            detection_ms = round((time.monotonic() - started) * 1000, 1)
            require(delivered[0]["revision"] == put["revision"], "watch missed the update")
        finally:
            stop.set()
    watch.close()
    require(cache.get()["value"] == "off" and cache.revision == put["revision"],
            "watch did not refresh hot key")
    require(baseline["strong_gets"] == count and cached["strong_gets"] < count - hot_count + 10,
            "cache should offload hot reads")
    return dict(requests=count, hot_percent=hot_percent, baseline=baseline, cached=cached,
                new_revision=put["revision"], watch_detection_ms=detection_ms)


def concurrent_cas(c):
    initial = c.put("flag", "old", "setup")
    gate = Barrier(2)

    def contender(value):
        gate.wait(timeout=5)
        return c.put("flag", value, f"cas-{value}", initial["revision"])

    with ThreadPoolExecutor(max_workers=2) as pool:
        submissions = [pool.submit(contender, value) for value in ("blue", "green")]
        results = [future.result() for future in submissions]
    require(sum(result["ok"] for result in results) == 1, "CAS must have exactly one winner")
    state = c.get("flag")
    require(state["value"] == ("blue" if results[0]["ok"] else "green"), "wrong CAS winner")
    _, events = c.watch("flag", 0)
    require(len(events) == 2 and state["revision"] == 2, "failed CAS generated KV change")
    return dict(expected_revision=1, accepted=1, rejected=1, final_value=state["value"],
                final_key_revision=2, commit_index=c.watermark())


class TestClock:
    def __init__(self):
        self.wall = 1000
        self.mono = 0

    def time(self):
        return self.wall

    def monotonic(self):
        return self.mono


def clock_rollback(c):
    clock = TestClock()
    c.put("flag", "old", "init")
    cache = Cache(c, "flag", ttl=1.0, monotonic=clock.monotonic)
    cache.get()
    with patch("time.time", side_effect=clock.time):
        before = time.time()
        clock.wall -= 60
        require(time.time() == before - 60, "wall clock did not roll back")
        updated = c.put("flag", "new", "after-rollback")
    clock.mono = 0.99
    require(cache.get()["value"] == "old", "cache TTL not respected")
    c.partition("A", True)
    clock.mono = 1.01
    expect(Unavailable, cache.get)
    c.partition("A", False)
    require(cache.get()["value"] == "new" and cache.revision == updated["revision"],
            "refresh or revision failed after clock rollback")
    return dict(wall_shift_s=-60, ttl_s=1, expired_at_s=1.01, timeout_closed=True,
                revisions=[1, updated["revision"]])


def lost_reply(c):
    expect(ReplyLost, c.put, "flag", "on", "client-42", lose_reply=True)
    committed = c.get("flag")
    c.stop("A")
    c.start("A")
    retried = c.put("flag", "on", "client-42")
    _, events = c.watch("flag", 0)
    require(retried == {"ok": True, "revision": committed["revision"]}, "retry response changed")
    require(len(events) == 1 and c.watermark() == 1, "retry committed twice")
    return dict(request_id="client-42", attempts=2, commits=1, events=1, revision=1)


SCENARIOS = {
    "01_replica_crash": replica_crash,
    "02_partition_duplicates": partition_duplicates,
    "03_delayed_message": delayed_message,
    "04_memory_loss": memory_loss,
    "05_learner_crash": learner_crash,
    "06_hotspot": hotspot,
    "07_concurrent_cas": concurrent_cas,
    "08_clock_rollback": clock_rollback,
    "09_lost_reply": lost_reply,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=SCENARIOS)
    parser.add_argument("--out", type=Path, default=Path("results"))
    args = parser.parse_args()
    cases = {args.case: SCENARIOS[args.case]} if args.case else SCENARIOS
    rows = []
    with tempfile.TemporaryDirectory(prefix="config-kv-") as temp:
        for name, scenario in cases.items():
            started = time.monotonic()
            with Cluster(Path(temp) / name) as cluster:
                details = scenario(cluster)
            if name == "06_hotspot":
                with Cluster(Path(temp) / "hotspot_50pct") as cluster:
                    details["sensitivity_50pct"] = hotspot(cluster, 50)
            row = dict(case=name, status="PASS", elapsed_ms=round((time.monotonic() - started) * 1000, 1),
                       details=details)
            rows.append(row)
            print(json.dumps(row, ensure_ascii=False), flush=True)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "results.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    with (args.out / "results.csv").open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=("case", "status", "elapsed_ms", "details"))
        writer.writeheader()
        for row in rows:
            writer.writerow(row | {"details": json.dumps(row["details"], ensure_ascii=False)})


if __name__ == "__main__":
    main()
