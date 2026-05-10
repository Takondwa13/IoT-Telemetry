"""Entry point for the WebSocket live-feed server.

Run with:
    python -m wss
"""
from __future__ import annotations

import asyncio
import logging

import websockets

from wss.broadcaster import Broadcaster
from wss import handler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global broadcaster instance
broadcaster: Broadcaster | None = None


async def main() -> None:
    """Boot the WebSocket server.

    Responsibilities:
      - Construct the broadcaster.
      - Subscribe to the source of incoming readings (shared queue, DB poll,
        IPC channel — your design decision).
      - Start the WebSocket server on the configured host/port.
      - Run forever.
    """
    global broadcaster
    
    # Build Broadcaster
    broadcaster = Broadcaster()
    logger.info("Initialized Broadcaster")
    
    # Set the broadcaster in the handler module
    handler.set_broadcaster(broadcaster)
    
    # Configuration
    wss_host = "127.0.0.1"
    wss_port = 8080
    
    # Start WebSocket server
    logger.info(f"Starting WebSocket server on {wss_host}:{wss_port}")
    
    async with websockets.serve(handler.live, wss_host, wss_port):
        logger.info(f"WebSocket server listening on ws://{wss_host}:{wss_port}/live")
        
        # Await indefinitely
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
