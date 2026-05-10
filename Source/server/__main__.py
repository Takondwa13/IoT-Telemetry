"""Entry point for the telemetry server.

Run with:
    python -m server
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import Optional

from aiohttp import web

from server.rest_api import build_app
from server.storage import InMemoryStorage
from server.tcp_ingest import start_tcp_server
from wss.broadcaster import Broadcaster

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Boot the telemetry server.

    Responsibilities:
      - Initialise the storage layer.
      - Start the TCP ingest listener for sensor connections.
      - Start the aiohttp app hosting the REST API.
      - Wait until shutdown.
    """
    # Build the Storage instance
    storage = InMemoryStorage()
    logger.info("Initialized in-memory storage")
    
    # Build the Broadcaster for live WebSocket updates
    broadcaster = Broadcaster()
    logger.info("Initialized Broadcaster")
    
    # Start the TCP ingest server (server.tcp_ingest.start_tcp_server)
    tcp_host = "127.0.0.1"
    tcp_port = 9000
    tcp_server = await start_tcp_server(tcp_host, tcp_port, storage, broadcaster)
    logger.info(f"Started TCP ingest server on {tcp_host}:{tcp_port}")
    
    # Start the aiohttp REST app (server.rest_api.build_app)
    app = build_app(storage)
    runner = web.AppRunner(app)
    await runner.setup()
    
    rest_host = "127.0.0.1"
    rest_port = 8000
    site = web.TCPSite(runner, rest_host, rest_port)
    await site.start()
    logger.info(f"Started REST API server on http://{rest_host}:{rest_port}")
    
    # Event to signal shutdown
    shutdown_event: asyncio.Event = asyncio.Event()
    
    def signal_handler(signum: int, frame: Optional[object]) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        shutdown_event.set()
    
    # Register signal handlers - use portable approach
    # On Windows, only SIGTERM is supported, but we'll handle KeyboardInterrupt instead
    if sys.platform != "win32":
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    else:
        signal.signal(signal.SIGTERM, signal_handler)
    
    tcp_server_task = asyncio.create_task(tcp_server.serve_forever())
    shutdown_task = asyncio.create_task(shutdown_event.wait())
    
    try:
        # Run TCP server and wait for shutdown signal
        done, pending = await asyncio.wait(
            {tcp_server_task, shutdown_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        # If shutdown_event was set, cancel the TCP server task
        if shutdown_task in done:
            tcp_server_task.cancel()
            try:
                await tcp_server_task
            except asyncio.CancelledError:
                pass
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        tcp_server_task.cancel()
        try:
            await tcp_server_task
        except asyncio.CancelledError:
            pass
    finally:
        tcp_server.close()
        await tcp_server.wait_closed()
        await runner.cleanup()
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
