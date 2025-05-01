#!/usr/bin/env python
# encoding: utf-8
"""
Created by Rui Carmo on 2013-03-12
Published under the MIT license.
"""

import os, sys, logging
from miniredis.client import RedisClient
from multiprocessing import Pool, current_process
import time
import random

log = logging.getLogger()

# Assume server is running on localhost:6379 or configure as needed
REDIS_HOST = "localhost"
REDIS_PORT = 6379

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(processName)s - %(levelname)s - %(message)s",
    )

    def timed_worker(args):
        count, db_index = args
        worker_name = current_process().name
        c = None
        ops_done = 0
        try:
            log.info(f"{worker_name}: Connecting to {REDIS_HOST}:{REDIS_PORT}")
            c = RedisClient(host=REDIS_HOST, port=REDIS_PORT)
            c.select(db_index)  # Use a different DB per worker if desired
            log.info(f"{worker_name}: Selected DB {db_index}")

            # Use a range relevant to the number of operations
            key_range = list(range(0, max(10000, count // 2)))  # Adjust key space size
            if not key_range:
                log.warning(
                    f"{worker_name}: Key range is empty, cannot perform operations."
                )
                return 0.0, 0  # Return time and ops count

            log.info(f"{worker_name}: Starting {count} SET/GET operations...")
            start_time = time.monotonic()

            for i in range(count):
                try:
                    # Generate key within the pool for this worker
                    key_suffix = random.choice(key_range)
                    key = f"bench:{worker_name}:{key_suffix}"
                    value = f"v-{i}"

                    c.set(key, value)
                    ops_done += 1

                    # Occasionally get a key
                    if i % 5 == 0:
                        get_key_suffix = random.choice(key_range)
                        get_key = f"bench:{worker_name}:{get_key_suffix}"
                        c.get(get_key)
                        ops_done += 1

                except ConnectionError as ce:
                    log.error(
                        f"{worker_name}: Connection error during operation {i}: {ce}"
                    )
                    raise  # Re-raise to stop this worker
                except Exception as e:
                    log.error(
                        f"{worker_name}: Error during operation {i} (key: {key}): {e}"
                    )
                    # Decide whether to continue or stop
                    # continue
                    raise  # Re-raise to stop this worker

            elapsed_time = time.monotonic() - start_time
            log.info(
                f"{worker_name}: Finished {ops_done} operations in {elapsed_time:.4f} seconds."
            )
            return elapsed_time, ops_done

        except ConnectionRefusedError:
            log.error(
                f"{worker_name}: Connection refused. Is the miniredis server running on {REDIS_HOST}:{REDIS_PORT}?"
            )
            return float("inf"), ops_done  # Indicate failure
        except Exception as e:
            log.error(f"{worker_name}: Unhandled exception in worker: {e}")
            return float("inf"), ops_done  # Indicate failure
        finally:
            if c:
                try:
                    c.close()
                    log.info(f"{worker_name}: Connection closed.")
                except Exception as e:
                    log.error(f"{worker_name}: Error closing connection: {e}")

    num_workers = 4
    # Total operations roughly split among workers
    # Let's aim for ~100k total ops (SETs + some GETs)
    total_target_ops = 100000
    # Estimate ops per worker (mostly SETs, some GETs)
    # Each loop does 1 SET and sometimes 1 GET (avg 1.2 ops/loop)
    loops_per_worker = int(total_target_ops / num_workers / 1.2)

    worker_args = [
        (loops_per_worker, i) for i in range(num_workers)
    ]  # Assign DB index i to worker i

    log.info(
        f"Starting benchmark with {num_workers} workers, approx {loops_per_worker} loops each."
    )

    total_time = 0.0
    total_ops_completed = 0
    successful_workers = 0

    # Use try-except around the Pool to catch potential setup issues
    try:
        with Pool(num_workers) as p:
            results = p.map(timed_worker, worker_args)

        for elapsed, ops_count in results:
            if elapsed != float("inf"):
                total_time += elapsed
                total_ops_completed += ops_count
                successful_workers += 1
            else:
                log.warning("A worker failed to complete.")

    except Exception as e:
        log.error(f"Error during multiprocessing pool execution: {e}")
        # Exit or handle as appropriate
        sys.exit(1)

    log.info("Benchmark finished.")

    if successful_workers > 0 and total_time > 0:
        # Calculate ops/sec based on the sum of time spent by successful workers
        # and the total operations they completed.
        ops_sec = total_ops_completed / total_time
        print(f"\n--- Benchmark Summary ---")
        print(f"Successful Workers: {successful_workers}/{num_workers}")
        print(f"Total Operations Completed: {total_ops_completed}")
        print(f"Total Worker CPU Time: {total_time:.4f} seconds")
        print(f"Aggregate Operations/Second: {ops_sec:.2f}")
        print(f"(Note: This is aggregate throughput, not single-client latency)")
    elif successful_workers == 0:
        print("\n--- Benchmark Failed ---")
        print(
            "All workers encountered errors. Please check logs and ensure the server is running."
        )
    else:  # total_time is 0 or total_ops_completed is 0
        print("\n--- Benchmark Result ---")
        print("No time elapsed or no operations completed by successful workers.")

    # Optional: Clean up keys used by benchmark
    # try:
    #     log.info("Cleaning up benchmark keys...")
    #     c = RedisClient(host=REDIS_HOST, port=REDIS_PORT)
    #     for i in range(num_workers):
    #         c.select(i)
    #         keys_to_delete = c.keys(f'bench:PoolWorker-{i+1}:*') # Adjust pattern if worker names differ
    #         if keys_to_delete:
    #             c.delete(*keys_to_delete)
    #     c.close()
    #     log.info("Cleanup complete.")
    # except Exception as e:
    #     log.error(f"Error during cleanup: {e}")
