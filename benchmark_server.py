#!/usr/bin/env python
# encoding: utf-8
"""
First modified by Rui Carmo on 2013-03-12
Published under the MIT license.
"""

import os, sys, logging, signal
from miniredis.server import RedisServer

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# Global server instance for signal handling
server_instance = None


def shutdown_handler(signum, frame):
    """Gracefully shut down the server on SIGINT or SIGTERM."""
    log.info(f"Received signal {signal.Signals(signum).name}. Shutting down...")
    if server_instance:
        server_instance.stop()
    # The run loop should exit after stop() sets halt=True
    # sys.exit(0) # Avoid exiting here, let the main loop finish


def main():
    global server_instance
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # TODO: Add argument parsing for host, port, db_path, etc.
    host = "127.0.0.1"
    port = 6379
    db_path = "."

    log.info(f"Starting miniredis server on {host}:{port}")
    log.info(f"Database path: {os.path.abspath(db_path)}")
    server_instance = RedisServer(host=host, port=port, db_path=db_path)

    try:
        server_instance.run()
    except Exception as e:
        log.exception("An unexpected error occurred in the server run loop")
    finally:
        log.info("Server has stopped.")
        # stop() should handle saving, but ensure it's called if not via signal
        if server_instance and not server_instance.halt:
            log.info("Performing final shutdown sequence.")
            server_instance.stop()

    sys.exit(0)


if __name__ == "__main__":
    main()
