"""A fixed-leader configuration store with durable replica processes.

Cluster is the only client-facing coordinator. The Python Pipe transport and
fault controls are intended for local experiments, not untrusted clients.
"""
from dataclasses import asdict, dataclass
import json
import multiprocessing
from pathlib import Path
import sqlite3
from threading import RLock
import time


class Unavailable(RuntimeError):
    """The coordinator cannot contact the leader and a voting majority."""


class ReplyLost(TimeoutError):
    """The write committed and the response was intentionally dropped."""


class Conflict(ValueError):
    """A request ID was reused for different arguments."""


@dataclass(frozen=True)
class Command:
    key: str
    value: str
    request_id: str
    expected: int | None


class ReplicaDB:
    """Indexed log and atomic materialization on the leader."""

    def __init__(self, file):
        self.db = sqlite3.connect(file)
        self.db.execute("PRAGMA synchronous = FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS log (
                idx INTEGER PRIMARY KEY, entry TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY, value TEXT NOT NULL, revision INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS requests (
                id TEXT PRIMARY KEY, idx INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS committed (idx INTEGER NOT NULL);
            INSERT INTO committed SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM committed);
        """)

    def ping(self):
        return True

    def watermark(self):
        return self.db.execute("SELECT idx FROM committed").fetchone()[0]

    def last(self):
        return self.db.execute("SELECT COALESCE(MAX(idx), 0) FROM log").fetchone()[0]

    def log(self, first, last):
        return self.db.execute(
            "SELECT idx, entry FROM log WHERE idx BETWEEN ? AND ? ORDER BY idx",
            (first, last),
        ).fetchall()

    def get(self, key):
        row = self.db.execute("SELECT value, revision FROM state WHERE key = ?", (key,)).fetchone()
        return dict(value=row[0], revision=row[1]) if row else dict(value=None, revision=0)

    def request(self, request_id):
        row = self.db.execute(
            "SELECT log.entry FROM requests JOIN log ON log.idx = requests.idx WHERE requests.id = ?",
            (request_id,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def append(self, entries):
        """Append a contiguous batch or validate exact duplicates atomically."""
        last = self.last()
        with self.db:
            for index, entry in entries:
                if index <= last:
                    existing = self.db.execute("SELECT entry FROM log WHERE idx = ?", (index,)).fetchone()
                    if existing != (entry,):
                        raise ValueError(f"Conflicting entry at index {index}")
                    continue
                if index != last + 1:
                    raise ValueError(f"Gap before index {index}")
                self.db.execute("INSERT INTO log VALUES (?, ?)", (index, entry))
                last = index
        return last

    def prepare(self, command):
        index = self.last() + 1
        current = self.get(command["key"])["revision"]
        success = command["expected"] is None or command["expected"] == current
        record = dict(command=command, result=dict(ok=success, revision=index if success else current))
        self.append([(index, json.dumps(record, sort_keys=True, ensure_ascii=False))])
        return index

    def commit(self, index):
        if index != self.watermark() + 1:
            raise ValueError("Commit must follow the previous commit")
        record = json.loads(self.log(index, index)[0][1])
        command, result = record["command"], record["result"]
        with self.db:
            if result["ok"]:
                self.db.execute("""
                    INSERT INTO state VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET value=excluded.value, revision=excluded.revision
                """, (command["key"], command["value"], index))
            self.db.execute("INSERT INTO requests VALUES (?, ?)", (command["request_id"], index))
            self.db.execute("UPDATE committed SET idx = ?", (index,))
        return result

    def watch(self, key, after):
        until = self.watermark()
        events = []
        for index, raw in self.log(after + 1, until):
            record = json.loads(raw)
            command = record["command"]
            if record["result"]["ok"] and command["key"] == key:
                events.append(dict(key=key, value=command["value"], revision=index))
        return until, events


def serve_replica(file, conn):
    db = ReplicaDB(file)
    with conn:
        while True:
            try:
                method, args = conn.recv()
            except EOFError:
                return
            try:
                reply = (True, getattr(db, method)(*args))
            except Exception as error:
                reply = (False, str(error))
            try:
                conn.send(reply)
            except (BrokenPipeError, EOFError, OSError):
                return


class Replica:
    def __init__(self, path):
        self.path = path
        self.ctx = multiprocessing.get_context("spawn")
        self.proc = None
        self.conn = None

    @property
    def alive(self):
        return self.proc is not None and self.proc.is_alive()

    @property
    def pid(self):
        return self.proc.pid

    def start(self):
        if self.alive:
            return
        self.stop()
        self.conn, child = self.ctx.Pipe()
        self.proc = self.ctx.Process(target=serve_replica, args=(self.path, child), daemon=True)
        self.proc.start()
        child.close()
        self.call("ping")

    def stop(self):
        if self.proc is not None:
            if self.proc.is_alive():
                self.proc.kill()
            self.proc.join(timeout=5)
            self.proc.close()
            self.proc = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def call(self, method, *args):
        if not self.alive:
            raise Unavailable(f"Replica {self.path.stem} is down")
        try:
            self.conn.send((method, args))
            if not self.conn.poll(5):
                # Discard a wedged channel so a late response cannot pair with the next call.
                self.stop()
                raise Unavailable(f"Replica {self.path.stem} timed out")
            ok, result = self.conn.recv()
        except (EOFError, OSError) as error:
            raise Unavailable(f"Replica {self.path.stem} disconnected") from error
        if not ok:
            raise RuntimeError(f"{self.path.stem}.{method}: {result}")
        return result


class Cluster:
    """A is fixed leader. A, B, C vote. A learner can copy only committed data."""

    VOTERS = ("A", "B", "C")

    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.nodes = {node: Replica(self.directory / f"{node}.sqlite") for node in self.VOTERS}
        self.lock = RLock()
        self.isolated = set()
        self.duplicates = 0
        self.replication_rpcs = 0
        self.strong_gets = 0

    def __enter__(self):
        try:
            for node in self.VOTERS:
                self.start(node)
        except BaseException:
            self.__exit__()
            raise
        return self

    def __exit__(self, *_):
        for replica in self.nodes.values():
            replica.stop()

    def start(self, name):
        with self.lock:
            self.nodes[name].start()

    def stop(self, name):
        with self.lock:
            self.nodes[name].stop()

    def partition(self, name, enabled):
        with self.lock:
            (self.isolated.add if enabled else self.isolated.discard)(name)

    def can_talk(self, src, dst):
        return self.nodes[src].alive and self.nodes[dst].alive and (
            src == dst or (src not in self.isolated and dst not in self.isolated)
        )

    @property
    def leader(self):
        return self.nodes["A"]

    def _quorum(self):
        if not self.leader.alive:
            raise Unavailable("A is down")
        peers = []
        for name in self.VOTERS[1:]:
            if not self.can_talk("A", name):
                continue
            try:
                self.nodes[name].call("ping")
            except Unavailable:
                continue
            peers.append(name)
        if not peers:
            raise Unavailable("A cannot reach a second voter")
        return peers

    def deliver(self, name, entries):
        with self.lock:
            if name == "A" or not self.can_talk("A", name):
                raise Unavailable(f"Cannot replicate from A to {name}")
            self.replication_rpcs += 1
            return self.nodes[name].call("append", entries)

    def _copy(self, name, end, batch_size=None, replay=0):
        if name == "A" or not self.can_talk("A", name):
            raise Unavailable(f"Cannot copy from A to {name}")
        first = self.nodes[name].call("last") + 1
        if first > end:
            return
        if batch_size is not None:
            end = min(end, first + batch_size - 1)
        entries = self.leader.call("log", first, end)
        for _ in range(replay + 1):
            self.deliver(name, entries)

    def _finish(self, index, peers):
        for name in peers:
            try:
                self._copy(name, index, replay=self.duplicates)
            except Unavailable:
                continue
            if self.nodes[name].call("last") >= index:
                return self.leader.call("commit", index)
        raise Unavailable("No follower acknowledged the durable log entry")

    def put(self, key, value, request_id, expected=None, *, lose_reply=False):
        if not isinstance(key, str) or not key or not isinstance(value, str):
            raise ValueError("Expected a nonempty key and a string value")
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("Expected a nonempty request ID")
        if expected is not None and (type(expected) is not int or expected < 0):
            raise ValueError("Expected a nonnegative revision")
        command = asdict(Command(key, value, request_id, expected))
        with self.lock:
            peers = self._quorum()
            tail = self.leader.call("last")
            if tail > self.leader.call("watermark"):
                self._finish(tail, peers)
            earlier = self.leader.call("request", request_id)
            if earlier:
                if earlier["command"] != command:
                    raise Conflict(f"Request ID already used: {request_id}")
                result = earlier["result"]
            else:
                index = self.leader.call("prepare", command)
                result = self._finish(index, peers)
            if lose_reply:
                raise ReplyLost(request_id)
            return result

    def get(self, key):
        with self.lock:
            self._quorum()
            self.strong_gets += 1
            return self.leader.call("get", key) | {"cursor": self.leader.call("watermark")}

    def watch(self, key, after):
        with self.lock:
            self._quorum()
            return self.leader.call("watch", key, after)

    def watermark(self):
        with self.lock:
            return self.leader.call("watermark")

    def sync(self, name):
        with self.lock:
            if name not in self.VOTERS[1:]:
                raise ValueError("sync expects a voting follower")
            self._copy(name, self.watermark())

    def add_learner(self, name):
        with self.lock:
            if not name.isalnum() or name in self.nodes:
                raise ValueError("Invalid or already registered learner name")
            self.nodes[name] = Replica(self.directory / f"{name}.sqlite")
            self.start(name)

    def migrate(self, name, up_to, *, limit=None):
        with self.lock:
            if name in self.VOTERS or not 0 <= up_to <= self.watermark():
                raise ValueError("Migration needs a learner and a committed target")
            if limit is not None and limit < 1:
                raise ValueError("Batch limit must be positive")
            self._copy(name, up_to, batch_size=limit)

    def learner_ready(self, name):
        with self.lock:
            return name not in self.VOTERS and self.can_talk("A", name) and (
                self.nodes[name].call("last") == self.watermark()
            )


class Cache:
    """Opt-in bounded-age cache with resumable per-key Watch polling."""

    def __init__(self, cluster, key, ttl=1.0, monotonic=time.monotonic):
        self.cluster = cluster
        self.key = key
        self.ttl = ttl
        self.monotonic = monotonic
        self.value = None
        self.revision = 0
        self.cursor = 0
        self.expires = float("-inf")
        self.lock = RLock()

    def get(self):
        with self.lock:
            now = self.monotonic()
            if now >= self.expires:
                current = self.cluster.get(self.key)
                self.value, self.revision = current["value"], current["revision"]
                self.cursor = current["cursor"]
                self.expires = now + self.ttl
            return dict(value=self.value, revision=self.revision)

    def apply(self, event):
        with self.lock:
            if event["key"] != self.key or event["revision"] <= self.revision:
                return False
            self.revision, self.value = event["revision"], event["value"]
            # A delayed watch event cannot extend the cache's freshness budget.
            return True

    def poll(self):
        with self.lock:
            cursor, events = self.cluster.watch(self.key, self.cursor)
            for event in events:
                self.apply(event)
            self.cursor = cursor
            return events


    def follow(self, stop, interval=0.05):
        """Yield batches of changes until the caller sets its threading.Event."""
        if interval <= 0:
            raise ValueError("Watch interval must be positive")
        while not stop.is_set():
            try:
                events = self.poll()
            except Unavailable:
                # Keep the last cursor so the next successful poll fills the gap.
                events = []
            if events:
                yield events
            stop.wait(interval)
