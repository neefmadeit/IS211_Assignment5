from __future__ import annotations

import argparse
import csv
import io
import sys
from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional, Tuple
from urllib.parse import urlparse
from urllib.request import urlopen

@dataclass
class Request:
    arrival_time: int
    resource: str
    process_time: int
    start_time: Optional[int] = None
    completion_time: Optional[int] = None

class Server:
    def __init__(self) -> None:
        self.current: Optional[Request] = None
        self.time_remaining: int = 0
    def tick(self, current_time: int) -> None:
        if self.current is not None:
            self.time_remaining -= 1
            if self.time_remaining <= 0:
                self.current.completion_time = current_time
                self.current = None
    def busy(self) -> bool:
        return self.current is not None
    def start_next(self, req: Request, current_time: int) -> None:
        self.current = req
        self.time_remaining = req.process_time
        req.start_time = current_time

# ------------- Input -------------
def _read_text_from_source(source: str) -> str:
    """Return file contents as text from local path, URL, or stdin ('-')."""
    if source == '-':
        return sys.stdin.read()
    parsed = urlparse(source)
    if parsed.scheme in ("http", "https"):
        with urlopen(source) as resp:
            raw = resp.read()
        try:
            return raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            return raw.decode("latin-1")
    else:
        with open(source, "r", encoding="utf-8-sig") as f:
            return f.read()

def read_requests_from_source(source: str) -> List[Request]:
    text = _read_text_from_source(source)
    reader = csv.reader(io.StringIO(text), skipinitialspace=True)
    requests: List[Request] = []
    for row in reader:
        if not row or all(not cell.strip() for cell in row):
            continue
        if len(row) < 3:
            row = [c.strip() for c in ",".join(row).split(",")]
            if len(row) < 3:
                continue
        a, path, p = row[0].strip(), row[1].strip(), row[2].strip()
        try:
            arrival = int(a)
            proc = int(p)
        except ValueError:
            continue
        requests.append(Request(arrival_time=arrival, resource=path, process_time=proc))
    requests.sort(key=lambda r: r.arrival_time)
    return requests

# ------------- Simulation -------------
def _run_timeline(arrivals: List[Request], server_count: int = 1) -> Tuple[float, float, int, int]:
    if server_count < 1:
        raise ValueError("server_count must be >= 1")
    n = len(arrivals)
    if n == 0:
        return 0.0, 0.0, 0, 0
    servers = [Server() for _ in range(server_count)]
    from collections import deque
    queues: List[Deque[Request]] = [deque() for _ in range(server_count)]
    idx = 0
    rr_idx = 0
    total_wait = 0
    total_turnaround = 0
    completed = 0
    current = 0
    while True:
        while idx < n and arrivals[idx].arrival_time == current:
            queues[rr_idx].append(arrivals[idx])
            rr_idx = (rr_idx + 1) % server_count
            idx += 1
        for s, q in zip(servers, queues):
            if (not s.busy()) and q:
                req = q.popleft()
                wait = max(0, current - req.arrival_time)
                total_wait += wait
                s.start_next(req, current)
        for s in servers:
            prev_req = s.current
            s.tick(current_time=current + 1)
            if prev_req is not None and s.current is None:
                req = prev_req
                total_turnaround += (req.completion_time or (current + 1)) - req.arrival_time
                completed += 1
        if idx >= n and all(len(q) == 0 for q in queues) and all(not s.busy() for s in servers):
            last_time = current + 1
            break
        current += 1
        if current > 10_000_000:
            raise RuntimeError("Simulation exceeded maximum time steps; check input data.")
    avg_wait = total_wait / n if n else 0.0
    avg_turnaround = total_turnaround / n if n else 0.0
    return avg_wait, avg_turnaround, completed, last_time

def simulateOneServer(source: str) -> float:
    arrivals = read_requests_from_source(source)
    avg_wait, avg_turn, completed, last_time = _run_timeline(arrivals, server_count=1)
    print(f"Requests processed: {completed}")
    print(f"Simulation length: {last_time} seconds")
    print(f"Average wait time: {avg_wait:.6f} seconds")
    print(f"Average turnaround time (wait + service): {avg_turn:.6f} seconds")
    return avg_wait

def simulateManyServers(source: str, servers: int) -> float:
    if servers < 1:
        raise ValueError("servers must be >= 1")
    arrivals = read_requests_from_source(source)
    avg_wait, avg_turn, completed, last_time = _run_timeline(arrivals, server_count=servers)
    print(f"Servers: {servers}")
    print(f"Requests processed: {completed}")
    print(f"Simulation length: {last_time} seconds")
    print(f"Average wait time: {avg_wait:.6f} seconds")
    print(f"Average turnaround time (wait + service): {avg_turn:.6f} seconds")
    return avg_wait

# ------------- CLI -------------
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=(
        "Simulate web request processing with one or many servers. "
        "Source can be a local CSV path, '-' for stdin, or an HTTP/HTTPS URL."
    ))
    # New positional argument for source
    p.add_argument("source", nargs="?", help="CSV source: path, URL, or '-' for stdin")
    # Backward-compat optional --file
    p.add_argument("--file", dest="file_opt", help="(Deprecated) CSV source path/URL (use positional 'source')")
    p.add_argument("--servers", type=int, default=None, help="If >1, run multi-server round-robin; else single server.")
    return p

def main(argv: Optional[List[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    # Resolve source: prefer positional; fall back to --file
    source = args.source or args.file_opt
    if not source:
        parser.error("missing CSV source. Provide a path/URL as positional arg or use --file.")

    servers = args.servers
    if servers is None or servers == 1:
        simulateOneServer(source)
    else:
        simulateManyServers(source, servers)

if __name__ == "__main__":
    main()