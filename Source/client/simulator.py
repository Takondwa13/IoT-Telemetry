"""Single-sensor simulation logic.

Each simulated sensor:
  - Connects to the telemetry server over TCP.
  - Generates plausible readings on its configured interval.
  - Encodes each reading as a Protobuf message and writes a length-prefixed
    frame on the socket.
  - Reconnects with backoff after transient network failures.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any

from google.protobuf.timestamp_pb2 import Timestamp

from proto import telemetry_pb2

logger = logging.getLogger(__name__)


class SensorSimulator:
    """Simulates one sensor pushing readings to the telemetry server."""

    # Backoff configuration
    INITIAL_BACKOFF_SECONDS = 1.0
    MAX_BACKOFF_SECONDS = 60.0
    BACKOFF_MULTIPLIER = 2.0

    def __init__(
        self,
        sensor_id: str,
        sensor_type: str,
        interval_seconds: float,
        host: str,
        port: int,
        **kwargs: Any,
    ) -> None:
        """Initialize a sensor simulator.
        
        Args:
            sensor_id: Unique identifier for this sensor
            sensor_type: Type of sensor (temperature, humidity, soil_moisture, light)
            interval_seconds: Reporting cadence in seconds
            host: Server hostname/IP
            port: Server TCP port
            **kwargs: Additional per-sensor configuration (min_value, max_value, drift_rate, etc.)
        """
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.interval_seconds = interval_seconds
        self.host = host
        self.port = port
        
        # Store per-type state for generating plausible readings
        self.min_value = kwargs.get("min_value", 0.0)
        self.max_value = kwargs.get("max_value", 100.0)
        self.drift_rate = kwargs.get("drift_rate", 0.1)
        
        # Initialize current reading as midpoint with random walk seed
        self.current_value = (self.min_value + self.max_value) / 2.0
        self.drift_seed = random.random() * 2 - 1  # Random value in [-1, 1]

    async def run(self) -> None:
        """Connect, then push readings on the configured interval forever."""
        backoff_seconds = self.INITIAL_BACKOFF_SECONDS
        
        # Outer loop with reconnect/backoff
        while True:
            try:
                await self._run_with_connection()
                # Reset backoff on successful connection
                backoff_seconds = self.INITIAL_BACKOFF_SECONDS
            except Exception as e:
                logger.error(
                    f"Sensor {self.sensor_id} connection error: {e}. "
                    f"Retrying in {backoff_seconds}s..."
                )
                await asyncio.sleep(backoff_seconds)
                # Exponential backoff with cap
                backoff_seconds = min(
                    backoff_seconds * self.BACKOFF_MULTIPLIER,
                    self.MAX_BACKOFF_SECONDS,
                )

    async def _run_with_connection(self) -> None:
        """Open TCP connection and run the inner loop."""
        reader, writer = await asyncio.open_connection(self.host, self.port)
        logger.info(f"Sensor {self.sensor_id} connected to {self.host}:{self.port}")
        
        try:
            # Inner loop: generate -> encode -> frame -> send -> sleep
            while True:
                # Generate and encode reading
                reading = self._generate_reading()
                encoded = reading.SerializeToString()
                
                # Write length-prefixed frame (4-byte big-endian length + data)
                frame = len(encoded).to_bytes(4, byteorder="big") + encoded
                
                # Send to server
                writer.write(frame)
                await writer.drain()
                
                logger.debug(
                    f"Sensor {self.sensor_id} sent reading: "
                    f"type={reading.reading_type}, value={reading.value}"
                )
                
                # Sleep until next reading
                await asyncio.sleep(self.interval_seconds)
        finally:
            writer.close()
            await writer.wait_closed()

    def _generate_reading(self) -> telemetry_pb2.Reading:
        """Produce a plausible next Reading for this sensor.
        
        Uses a random walk with drift to simulate realistic sensor behavior.
        """
        # Apply random walk with drift
        random_change = (random.random() * 2 - 1) * self.drift_rate
        self.drift_seed = self.drift_seed * 0.95 + random_change * 0.05
        
        # Update current value based on drift
        self.current_value += self.drift_seed
        
        # Clamp to min/max bounds
        self.current_value = max(self.min_value, min(self.max_value, self.current_value))
        
        # Build and return a Protobuf Reading message
        reading = telemetry_pb2.Reading()
        reading.sensor_id = self.sensor_id
        reading.reading_type = self.sensor_type
        reading.value = float(self.current_value)
        
        # Set timestamp to current Unix time
        timestamp = Timestamp()
        timestamp.FromDatetime(
            __import__("datetime").datetime.utcnow()
        )
        reading.timestamp.CopyFrom(timestamp)
        
        return reading
