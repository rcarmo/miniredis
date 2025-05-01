# tests/helpers.py
import os
import sys
import signal
import time
import socket
from multiprocessing import Process
from typing import Tuple
import miniredis.server
from miniredis.client import RedisClient

# Adjust path to import miniredis from the parent directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


def find_free_port() -> int:
    """Finds an available port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 0))
        return s.getsockname()[1]


def start_server() -> Tuple[Process, int]:
    """Starts the Redis server in a background process on a free port."""
    test_port: int = find_free_port()

    def run_server_process(port: int) -> None:
        # Signal handler for graceful shutdown within the child process
        def sigterm_handler(signum, frame):
            sys.exit(0)
        signal.signal(signal.SIGTERM, sigterm_handler)

        server: miniredis.server.RedisServer | None = None
        try:
            server = miniredis.server.RedisServer(port=port)
            print(f"Test server starting on port {port}...")
            server.run() # This blocks until server stops
        except KeyboardInterrupt:
            print("Test server received KeyboardInterrupt.")
        except Exception as e:
            print(f"Error in server process: {e}")
        finally:
            if server:
                print("Stopping test server...")
                server.stop()
            print(f"Test server on port {port} stopped.")

    # Start the server process
    proc = Process(target=run_server_process, args=(test_port,), daemon=True)
    proc.start()

    # Wait for the server to be ready by trying to connect
    max_wait: float = 5.0 # seconds
    start_wait: float = time.monotonic()
    connected: bool = False
    conn: socket.socket | None = None
    while time.monotonic() - start_wait < max_wait:
        if not proc.is_alive():
            raise RuntimeError(f"Server process {proc.pid} terminated unexpectedly.")
        try:
            # Try to establish a connection
            conn = socket.create_connection(('localhost', test_port), timeout=0.1)
            conn.close()
            connected = True
            print(f"Test server with pid {proc.pid} ready on port {test_port}.")
            break
        except (ConnectionRefusedError, socket.timeout):
            time.sleep(0.1) # Wait a bit before retrying
        finally:
            if conn:
                conn.close()

    if not connected:
        stop_server(proc) # Clean up the process if connection failed
        raise RuntimeError(f"Server process failed to start or become ready on port {test_port} within {max_wait}s.")

    return proc, test_port


def stop_server(proc: Process | None) -> None:
    """Stops the Redis server process."""
    if proc and proc.is_alive():
        print(f"Terminating test server with pid {proc.pid}.")
        proc.terminate() # Send SIGTERM
        proc.join(timeout=2) # Wait for graceful shutdown
        if proc.is_alive():
            print(f"Server process {proc.pid} did not terminate gracefully, killing.")
            proc.kill() # Force kill if terminate fails
            proc.join(timeout=1) # Wait briefly for kill
        print(f"Server process {proc.pid} stopped.")
    elif proc:
        print(f"Server process {proc.pid} already stopped (exitcode: {proc.exitcode}).")

