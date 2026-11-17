#!/usr/bin/env python3
"""Process-crash probe for persisted Iggy sends. Not a power-loss test."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid

TOKEN_RE = re.compile(r"r[0-9]{2}w[0-9]{2}n[0-9]{8}m[0-9]{2}")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iggy-root", type=Path, required=True,
                        help="built Apache Iggy checkout at the source pin under test")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="new directory for isolated data, logs and results")
    parser.add_argument("--profile", choices=("debug", "release"), default="debug",
                        help="Cargo build profile containing the binaries")
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--messages-per-group", type=int, default=4)
    parser.add_argument("--poll-limit", type=int, default=10000)
    args = parser.parse_args()
    if args.rounds < 1 or args.workers < 1 or not 1 <= args.messages_per_group <= 50:
        parser.error("rounds/workers must be positive and messages-per-group must be 1..50")
    root = args.iggy_root.resolve()
    output = args.output_dir.resolve()
    server_bin = root / f"target/{args.profile}/iggy-server"
    cli_bin = root / f"target/{args.profile}/iggy"
    if not server_bin.is_file() or not cli_bin.is_file():
        parser.error(f"expected target/{args.profile}/iggy-server and target/{args.profile}/iggy; build the pinned checkout first")
    output.mkdir(parents=True, exist_ok=False)
    data = output / "data"
    data.mkdir()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    address = f"127.0.0.1:{port}"
    stream = "loadprobe" + uuid.uuid4().hex[:6]
    topic = "persisted"
    env = os.environ.copy()
    env.update({
        "IGGY_PATH": str(data),
        "IGGY_HTTP_ENABLED": "false",
        "IGGY_TCP_ADDRESS": address,
        "IGGY_SHARDING_CPU_ALLOCATION": "1",
        "IGGY_SHARDING_PIN_CORES": "false",
        "IGGY_USERNAME": "iggy",
        "IGGY_PASSWORD": "iggy",
    })

    def client_args(*tail):
        return [str(cli_bin), "--tcp-server-address", address,
                "--tcp-reconnection-max-retries", "0",
                "--tcp-reconnection-interval", "100ms", *tail]

    def run_cli(*tail, timeout=8):
        return subprocess.run(client_args(*tail), env=env, capture_output=True,
                              text=True, timeout=timeout)

    log = open(output / "server.log", "a", buffering=1)
    server = None

    def start_server():
        nonlocal server
        server = subprocess.Popen([str(server_bin), "--with-default-root-credentials"],
                                  cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 25
        last = None
        while time.monotonic() < deadline:
            if server.poll() is not None:
                raise RuntimeError(f"server exited during startup: {server.returncode}")
            try:
                result = run_cli("ping", timeout=3)
                if result.returncode == 0:
                    return
                last = (result.returncode, result.stdout[-300:], result.stderr[-300:])
            except Exception as error:
                last = repr(error)
            time.sleep(0.15)
        raise RuntimeError(f"server did not become ready: {last!r}")

    def kill_server():
        nonlocal server
        if server is None or server.poll() is not None:
            raise RuntimeError("server not running at planned cut")
        server.kill()
        server.wait(timeout=5)
        if server.returncode != -signal.SIGKILL:
            raise RuntimeError(f"expected SIGKILL status {-signal.SIGKILL}, got {server.returncode}")
        server = None

    def read_markers():
        result = run_cli("message", "poll", "--first", "--message-count",
                         str(args.poll_limit), stream, topic, "0", timeout=45)
        if result.returncode:
            raise RuntimeError(f"poll failed: {result.stdout[-1000:]} {result.stderr[-1000:]}")
        return TOKEN_RE.findall(result.stdout + "\n" + result.stderr), result.stdout, result.stderr

    lock = threading.Lock()
    stop = threading.Event()
    sequence = 0
    acknowledged_groups = set()
    uncertain_groups = set()
    rounds = []
    server_log_path = output / "server.log"

    try:
        start_server()
        result = run_cli("stream", "create", stream)
        if result.returncode:
            raise RuntimeError(f"stream create failed: {result.stderr}")
        result = run_cli("topic", "create", stream, topic, "1", "none",
                         "--durability", "persisted",
                         "--consumer-offset-durability", "persisted")
        if result.returncode:
            raise RuntimeError(f"topic create failed: {result.stderr}")

        for round_no in range(1, args.rounds + 1):
            stop.clear()
            counts = {"acked": 0, "unknown": 0, "inflight": 0, "attempted": 0,
                      "started": time.monotonic()}

            def sender(worker_no):
                nonlocal sequence
                while not stop.is_set():
                    with lock:
                        sequence += 1
                        number = sequence
                        counts["attempted"] += 1
                        counts["inflight"] += 1
                    group = f"r{round_no:02d}w{worker_no:02d}n{number:08d}"
                    messages = [f"{group}m{idx:02d}" for idx in range(args.messages_per_group)]
                    try:
                        sent = run_cli("message", "send", stream, topic, *messages, timeout=4)
                        success = sent.returncode == 0
                    except Exception:
                        success = False
                    with lock:
                        counts["inflight"] -= 1
                        if success:
                            counts["acked"] += 1
                            acknowledged_groups.add(group)
                        else:
                            counts["unknown"] += 1
                            uncertain_groups.add(group)
                    time.sleep(0.002)

            threads = [threading.Thread(target=sender, args=(w,), daemon=True)
                       for w in range(1, args.workers + 1)]
            for thread in threads:
                thread.start()
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                with lock:
                    ready = counts["acked"] >= 2 and counts["inflight"] > 0
                if ready and time.monotonic() - counts["started"] >= 0.20:
                    break
                if server is None or server.poll() is not None:
                    raise RuntimeError("server exited before planned SIGKILL")
                time.sleep(0.001)
            else:
                raise RuntimeError(f"could not establish active send load: {counts}")
            with lock:
                inflight_at_kill = counts["inflight"]
                acked_before_kill = counts["acked"]
                attempted_before_kill = counts["attempted"]
            kill_server()
            stop.set()
            for thread in threads:
                thread.join(timeout=6)
            if any(thread.is_alive() for thread in threads):
                raise RuntimeError(f"producer did not exit after round {round_no}")
            start_server()
            payload_markers, poll_out, poll_err = read_markers()
            marker_counts = {token: payload_markers.count(token) for token in set(payload_markers)}
            expected_acked_messages = {
                f"{group}m{idx:02d}"
                for group in acknowledged_groups
                for idx in range(args.messages_per_group)
            }
            missing_or_duplicate = sorted(
                token for token in expected_acked_messages if marker_counts.get(token, 0) != 1
            )
            recovered_uncertain_groups = sorted(
                group for group in uncertain_groups
                if any(marker_counts.get(f"{group}m{idx:02d}", 0)
                       for idx in range(args.messages_per_group))
            )
            record = {
                "round": round_no,
                "server_exit_status": -signal.SIGKILL,
                "workers": args.workers,
                "inflight_groups_at_kill": inflight_at_kill,
                "groups_acked_this_round_before_kill": acked_before_kill,
                "send_groups_attempted_this_round_before_kill": attempted_before_kill,
                "groups_acked_total": len(acknowledged_groups),
                "groups_with_unknown_outcome_total": len(uncertain_groups),
                "messages_recovered_total": len(payload_markers),
                "unknown_outcome_groups_recovered": len(recovered_uncertain_groups),
                "acknowledged_messages_missing_or_not_unique": missing_or_duplicate,
                "result": "PASS" if not missing_or_duplicate else "FAIL",
            }
            rounds.append(record)
            (output / f"poll-round-{round_no:02d}.txt").write_text(poll_out + poll_err)
            (output / "results.json").write_text(json.dumps({
                "source_pin": "91e0206367aad2d30259f7d45203832b08ae84e7",
                "platform": sys.platform,
                "python": sys.version,
                "mode": f"{args.profile} server; persisted message and consumer-offset durability; singleton partition",
                "rounds_requested": args.rounds,
                "workers": args.workers,
                "messages_per_group": args.messages_per_group,
                "stream": stream,
                "topic": topic,
                "address": address,
                "rounds": rounds,
                "acknowledged_groups": sorted(acknowledged_groups),
                "unknown_outcome_groups": sorted(uncertain_groups),
            }, indent=2))
            print(json.dumps(record), flush=True)
            if missing_or_duplicate:
                break
        print(f"RESULTS={output / 'results.json'}")
        print(f"DATA={data}")
        print(f"ADDRESS={address}")
        print(f"STREAM={stream} TOPIC={topic}")
    finally:
        stop.set()
        if server is not None and server.poll() is None:
            kill_server()
        log.close()

if __name__ == "__main__":
    main()
