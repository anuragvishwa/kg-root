#!/usr/bin/env python
import os, socket, time, sys

if len(sys.argv) < 3:
    print("Usage: wait_kafka.py <hostname> <port>")
    sys.exit(1)

name = sys.argv[1]  # e.g. kafka
port = int(sys.argv[2])  # e.g. 9092

for i in range(60):  # max ~60 s
    try:
        print(f'[wait_dns] attempt {i+1}/60: trying {name}:{port} …')
        sock = socket.create_connection((name, port), timeout=2)
        sock.close()
        print(f'[wait_dns] SUCCESS: {name}:{port} is reachable — continuing')
        sys.exit(0)  # Explicit success exit
    except (OSError, socket.error) as e:
        print(f'[wait_dns] not up yet: {e}')
        if i < 59:  # Don't sleep on last iteration
            time.sleep(1)

print(f'[wait_dns] TIMEOUT: Could not connect to {name}:{port} after 60 seconds')
sys.exit(1)  # Explicit failure exit