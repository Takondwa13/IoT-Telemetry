"""Tracks connected WebSocket clients and dispatches readings to them.

Owns the set of live clients, their subscription filters, and a way for
producers (the telemetry server) to publish a new reading.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional, Set

logger = logging.getLogger(__name__)


class Broadcaster:
    """Fan-out of readings to the set of connected WebSocket clients."""

    def __init__(self) -> None:
        """Initialize broadcaster with empty client list and subscriptions."""
        # Track connected clients and their per-client subscriptions
        self.clients: dict[Any, Set[str]] = {}
        self.lock = asyncio.Lock()

    async def register(self, websocket: Any) -> None:
        """Add a newly connected client."""
        async with self.lock:
            # Default: subscribe to all sensors (empty filter means all)
            self.clients[websocket] = set()
            logger.info(f"Registered client: {id(websocket)}")

    async def unregister(self, websocket: Any) -> None:
        """Remove a disconnected client."""
        async with self.lock:
            if websocket in self.clients:
                del self.clients[websocket]
                logger.info(f"Unregistered client: {id(websocket)}")

    async def set_subscription(self, websocket: Any, sensor_ids: list[str]) -> None:
        """Replace the per-client sensor-id filter."""
        async with self.lock:
            if websocket in self.clients:
                self.clients[websocket] = set(sensor_ids) if sensor_ids else set()
                logger.info(
                    f"Updated subscription for {id(websocket)}: {sensor_ids or 'all'}"
                )

    async def publish(self, reading: Any) -> None:
        """Push a reading to every interested client.

        Strategy: Send concurrently with timeouts. Slow clients are
        disconnected to prevent stalling the broadcast.
        """
        # Serialize reading to JSON
        payload = {
            "sensor_id": reading.sensor_id,
            "reading_type": reading.reading_type,
            "value": reading.value,
            "timestamp": reading.timestamp.seconds,
        }
        message = json.dumps(payload)
        
        # Get current client list and subscriptions
        async with self.lock:
            clients_to_notify = []
            for websocket, subscribed_sensors in self.clients.items():
                # If subscription is empty (set()), match all sensors
                # Otherwise match only subscribed sensors
                if not subscribed_sensors or reading.sensor_id in subscribed_sensors:
                    clients_to_notify.append(websocket)
        
        # For each client whose subscription matches, send concurrently
        # with timeout to prevent slow clients from blocking
        if not clients_to_notify:
            return
        
        send_tasks = [
            self._send_with_timeout(websocket, message)
            for websocket in clients_to_notify
        ]
        
        results = await asyncio.gather(*send_tasks, return_exceptions=True)
        
        # Handle send-timeouts and disconnected clients
        for websocket, result in zip(clients_to_notify, results):
            if isinstance(result, Exception):
                logger.warning(
                    f"Failed to send to client {id(websocket)}: {result}. Disconnecting."
                )
                await self.unregister(websocket)
                try:
                    await websocket.close()
                except Exception as e:
                    logger.debug(f"Error closing websocket: {e}")

    async def _send_with_timeout(
        self, websocket: Any, message: str, timeout: float = 5.0
    ) -> None:
        """Send a message to a websocket with timeout."""
        try:
            await asyncio.wait_for(websocket.send(message), timeout=timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(f"Send timeout for client {id(websocket)}")
        except Exception as e:
            raise e
