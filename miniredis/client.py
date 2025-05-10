#!/usr/bin/env python
# encoding: utf-8
"""
Based on a minimalist Redis client originally written by Andrew Rowls

Created by Rui Carmo on 2013-03-12
Published under the MIT license.
"""

import logging
import random
import socket
import time
from multiprocessing import Pool
from typing import Any, Callable, List, Optional, Tuple, Union, TypeVar, cast, Dict
from typing import Protocol, Generic, ParamSpec

log = logging.getLogger()

# More advanced type variables for Python 3.10+
T = TypeVar("T")
P = ParamSpec("P")  # For handling arbitrary parameters in a generic way


# Define a Protocol for Redis command results
class RedisCommandResult(Protocol, Generic[T]):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...


class RedisClient:
    def __init__(self, host: str = "localhost", port: int = 6379) -> None:
        """Initialize a new Redis client connection.

        Args:
            host: Redis server hostname or IP
            port: Redis server port
        """
        self.sock = socket.create_connection((host, port))
        # Use binary mode for reading/writing
        self.file = self.sock.makefile("rwb", buffering=0)

    def __getattr__(self, attr: str) -> RedisCommandResult[Any]:
        # Map 'delete' attribute to 'DEL' command
        command = b"DEL" if attr == "delete" else attr.upper().encode("utf-8")

        def handle(*args: Any) -> Any:
            # Encode all arguments to bytes
            encoded_args: List[bytes] = []
            for a in args:
                if isinstance(a, bytes):
                    encoded_args.append(a)
                elif isinstance(a, str):
                    encoded_args.append(a.encode("utf-8"))
                else:
                    encoded_args.append(str(a).encode("utf-8"))

            # Build the command array
            cmd_parts = [command] + encoded_args
            cmd_str = f"*{len(cmd_parts)}\r\n".encode("utf-8")
            for part in cmd_parts:
                cmd_str += f"${len(part)}\r\n".encode("utf-8") + part + b"\r\n"

            # Send the command
            self.file.write(cmd_str)
            self.file.flush()  # Ensure command is sent immediately
            return self.parse_response()

        return handle

    def parse_response(self) -> Any:
        """Parse a Redis protocol response from the server.

        Returns:
            The parsed response in appropriate Python type

        Raises:
            ConnectionError: If the connection is closed
            Exception: For Redis errors or protocol violations
        """
        rsp = self.file.readline()
        if not rsp:
            # Connection closed or no response
            raise ConnectionError("Socket closed or no response received")

        type_byte, body = rsp[0:1], rsp[1:-2]  # Keep as bytes

        match type_byte:  # Using Python 3.10+ pattern matching
            case b"+":  # Simple String
                return body.decode("utf-8")
            case b"-":  # Error
                raise Exception(body.decode("utf-8"))
            case b":":  # Integer
                return int(body)
            case b"$":  # Bulk String
                length = int(body)
                return self.read_bulk(length)
            case b"*":  # Array
                count = int(body)
                if count == -1:
                    return None  # Null array
                return [self.parse_response() for _ in range(count)]
            case _:
                # Should not happen with a conforming server
                raise ValueError(
                    f'Unknown Return Value Type: "{type_byte.decode("utf-8", errors="backslashreplace")}"'
                )

    def read_bulk(self, n: int) -> Optional[bytes]:
        """Read a bulk string of specific length.

        Args:
            n: The length of the bulk string

        Returns:
            The string data or None for null bulk string

        Raises:
            ConnectionError: If insufficient data is read
            Exception: If protocol is violated
        """
        if n == -1:
            return None  # Null bulk string
        # Read exactly n bytes + 2 for CRLF
        data = self.file.read(n + 2)
        if len(data) < n + 2:
            raise ConnectionError("Incomplete bulk string read")
        if data[-2:] != b"\r\n":
            raise ValueError("Bulk string missing CRLF")
        return data[:-2]  # Return the data part as bytes

    def close(self) -> None:
        """Close the connection."""
        try:
            self.file.close()
            self.sock.close()
        except (socket.error, OSError) as e:
            log.debug(
                f"Error closing connection: {e}"
            )  # Log the error but don't propagate


if __name__ == "__main__":
    # Example usage and benchmark setup
    logging.basicConfig(level=logging.INFO)

    def timed(count: int) -> float:
        """Run a timed benchmark with GET/SET operations.

        Args:
            count: Number of GET/SET operation pairs to perform

        Returns:
            The elapsed time in seconds
        """
        c = None
        try:
            c = RedisClient()
            c.select(0)  # Select DB 0
            seq = list(range(0, 10000))
            # Pre-populate some keys
            for i in range(min(1000, len(seq))):
                k = str(random.choice(seq))
                c.set(k, "bar")

            now = time.time()
            for _ in range(count):
                k_get = str(random.choice(seq))
                try:
                    c.get(k_get)
                except Exception:
                    # Handle potential errors during GET (e.g., key not found is None, not Exception)
                    pass
                k_set = str(random.choice(seq))
                c.set(k_set, "bar")  # Set operation

            elapsed = time.time() - now
            return elapsed
        except Exception as e:
            log.error(f"Error in timed function: {e}")
            return float("inf")  # Indicate failure
        finally:
            if c is not None:  # Use is not None for more explicit comparison
                c.close()

    # Use more modern multiprocessing approach with context manager
    def run_benchmark(num_workers: int = 4, ops_per_worker: int = 10000) -> None:
        """Run a parallel benchmark with multiple workers.

        Args:
            num_workers: Number of parallel workers
            ops_per_worker: Operations per worker
        """
        total_ops = num_workers * ops_per_worker * 2  # *2 because we do GET and SET

        print(
            f"Running benchmark with {num_workers} workers, {ops_per_worker} GET/SET pairs each..."
        )

        with Pool(num_workers) as p:
            results = p.map(timed, [ops_per_worker] * num_workers)

        total_time = sum(r for r in results if r != float("inf"))
        successful_workers = sum(1 for r in results if r != float("inf"))

        if successful_workers > 0 and total_time > 0:
            # Calculate average time per worker, then overall ops/sec
            avg_time_per_worker = total_time / successful_workers
            # Estimate total ops based on successful workers
            estimated_total_ops = successful_workers * ops_per_worker * 2
            ops_sec = estimated_total_ops / total_time
            print(f"Benchmark finished.")
            print(f"Total operations (estimated): {estimated_total_ops}")
            print(f"Total time: {total_time:.4f} seconds")
            print(f"Operations per second: {ops_sec:.2f}")
        elif successful_workers == 0:
            print("Benchmark failed: All workers encountered errors.")
        else:  # total_time is 0, should not happen unless ops_per_worker is 0
            print("Benchmark finished, but no time elapsed or no operations performed.")

    # Run the benchmark with default parameters
    run_benchmark()
