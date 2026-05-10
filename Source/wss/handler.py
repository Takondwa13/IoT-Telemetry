"""WebSocket connection handler at /live.

One coroutine per connected client. Reads optional subscription messages
from the client and otherwise just forwards readings published by the
broadcaster.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Global broadcaster instance - set by the server
_broadcaster: Any = None


def set_broadcaster(broadcaster: Any) -> None:
    """Set the global broadcaster instance for this module."""
    global _broadcaster
    _broadcaster = broadcaster


async def live(websocket: Any, path: str) -> None:
    """Handle one WebSocket client connection.

    Protocol on this socket (JSON frames):
      Client -> Server (optional, after upgrade):
          {"action": "subscribe", "sensors": ["sensor-a", "sensor-b"]}
      Server -> Client (continuous):
          {"sensor_id": "...", "reading_type": "...", "value": ..., "timestamp": ...}
    """
    if not _broadcaster:
        logger.error("Broadcaster not initialized")
        await websocket.close()
        return
    
    # Register this client with the Broadcaster
    await _broadcaster.register(websocket)
    logger.info(f"Client connected: {websocket.remote_address}")
    
    try:
        # Read incoming subscription messages and update filters
        async for message in websocket:
            try:
                data = json.loads(message)
                
                if isinstance(data, dict):
                    action = data.get("action")
                    
                    if action == "subscribe":
                        sensors = data.get("sensors", [])
                        if isinstance(sensors, list):
                            await _broadcaster.set_subscription(websocket, sensors)
                            logger.info(
                                f"Client {websocket.remote_address} subscribed to: {sensors or 'all'}"
                            )
                    else:
                        logger.warning(
                            f"Unknown action from {websocket.remote_address}: {action}"
                        )
            except json.JSONDecodeError:
                logger.warning(
                    f"Received invalid JSON from {websocket.remote_address}: {message}"
                )
            except Exception as e:
                logger.error(
                    f"Error processing message from {websocket.remote_address}: {e}"
                )
    
    except asyncio.CancelledError:
        logger.info(f"Client {websocket.remote_address} connection cancelled")
    except Exception as e:
        logger.error(f"Error in client handler for {websocket.remote_address}: {e}")
    
    finally:
        # On disconnect, unregister cleanly
        await _broadcaster.unregister(websocket)
        logger.info(f"Client disconnected: {websocket.remote_address}")
