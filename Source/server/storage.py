"""Storage layer for sensors and readings.

The backing store is an implementation detail (in-memory dict, SQLite,
something else). The interface below is what the rest of the server uses.
"""
from __future__ import annotations
from proto import telemetry_pb2

from typing import Any, Iterable, Optional


class Storage:
    """Abstract storage interface."""

    async def add_sensor(self, sensor: Any) -> None:
        """Register a new sensor."""
        raise NotImplementedError

    async def remove_sensor(self, sensor_id: str) -> None:
        """Remove a sensor and (optionally) its readings."""
        raise NotImplementedError

    async def list_sensors(self) -> Iterable:
        """Return all registered sensors."""
        raise NotImplementedError

    async def add_reading(self, reading: Any) -> None:
        """Persist a single reading and auto-register the sensor if missing."""
        sensor_id = reading.sensor_id
        
        if sensor_id not in self.sensors:
            try:
                sensor = telemetry_pb2.SensorRegistration()
                sensor.sensor_id = sensor_id
                sensor.sensor_type = getattr(reading, 'reading_type', 'unknown')
                self.sensors[sensor_id] = sensor
                self.readings[sensor_id] = []
                logger.info(f"✅ Auto-registered sensor: {sensor_id} ({sensor.sensor_type})")
            except Exception as e:
                logger.error(f"Failed to auto-register {sensor_id}: {e}")
        
        self.readings[sensor_id].append(reading)
        logger.debug(f"Stored reading for {sensor_id}: {reading.value:.2f}")

    async def get_readings(
        self,
        sensor_id: str,
        from_ts: Optional[float] = None,
        to_ts: Optional[float] = None,
    ) -> Iterable:
        """Return readings for a sensor within an optional time window."""
        raise NotImplementedError


class InMemoryStorage(Storage):
    """In-memory storage implementation using dictionaries."""

    def __init__(self) -> None:
        """Initialize storage with empty sensor and reading dicts."""
        self.sensors: dict[str, Any] = {}
        self.readings: dict[str, list[Any]] = {}

    async def add_sensor(self, sensor: Any) -> None:
        """Register a new sensor."""
        sensor_id = sensor.sensor_id
        self.sensors[sensor_id] = sensor
        if sensor_id not in self.readings:
            self.readings[sensor_id] = []

    async def remove_sensor(self, sensor_id: str) -> None:
        """Remove a sensor and its readings."""
        if sensor_id in self.sensors:
            del self.sensors[sensor_id]
        if sensor_id in self.readings:
            del self.readings[sensor_id]

    async def list_sensors(self) -> list:
        """Return all registered sensors."""
        return list(self.sensors.values())

    async def add_reading(self, reading: Any) -> None:
        """Persist a single reading."""
        sensor_id = reading.sensor_id
        if sensor_id not in self.readings:
            self.readings[sensor_id] = []
        self.readings[sensor_id].append(reading)

    async def get_readings(
        self,
        sensor_id: str,
        from_ts: Optional[float] = None,
        to_ts: Optional[float] = None,
    ) -> list:
        """Return readings for a sensor within an optional time window."""
        if sensor_id not in self.readings:
            return []
        
        readings = self.readings[sensor_id]
        
        # Filter by timestamp if provided
        if from_ts is not None or to_ts is not None:
            filtered = []
            for reading in readings:
                ts = reading.timestamp.seconds
                if from_ts is not None and ts < from_ts:
                    continue
                if to_ts is not None and ts > to_ts:
                    continue
                filtered.append(reading)
            return filtered
        
        return readings
