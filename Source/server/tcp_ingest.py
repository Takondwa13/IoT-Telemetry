"""Asynchronous TCP listener for sensor connections."""

import asyncio
import logging
from typing import Any, Optional

from proto import telemetry_pb2

logger = logging.getLogger(__name__)

# Global storage and broadcaster - set by start_tcp_server
_storage: Optional[Any] = None
_broadcaster: Optional[Any] = None


async def handle_sensor(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """Handle one sensor connection until it closes."""
    peer_name = writer.get_extra_info("peername")
    sensor_id = "unknown"
    
    logger.info(f"Sensor connected from {peer_name}")
    
    try:
        while True:
            # Read 4-byte length prefix with timeout
            try:
                length_bytes = await asyncio.wait_for(reader.readexactly(4), timeout=60.0)
            except asyncio.TimeoutError:
                logger.warning(f"Sensor {sensor_id} timed out waiting for data")
                break
            except asyncio.IncompleteReadError:
                logger.info(f"Sensor {sensor_id} disconnected")
                break

            frame_length = int.from_bytes(length_bytes, byteorder="big")

            if frame_length == 0:
                continue
            if frame_length > 8192:  # Safety limit
                logger.warning(f"Frame too large ({frame_length} bytes) from {peer_name}")
                break

            # Read the protobuf payload
            try:
                frame_data = await asyncio.wait_for(
                    reader.readexactly(frame_length), timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"Timeout reading payload from {sensor_id}")
                break
            except asyncio.IncompleteReadError:
                logger.info(f"Sensor {sensor_id} disconnected during payload read")
                break

            try:
                # Decode Protobuf
                reading = telemetry_pb2.Reading()
                reading.ParseFromString(frame_data)
                
                sensor_id = reading.sensor_id  # Update for better logging

                # Store the reading
                if _storage:
                    await _storage.add_reading(reading)
                    logger.info(
                        f"✓ Received & stored: {reading.sensor_id} | "
                        f"{reading.reading_type} = {reading.value:.2f}"
                    )
                else:
                    logger.warning("Storage not configured!")

                # Publish to live broadcaster
                if _broadcaster:
                    await _broadcaster.publish(reading)

            except Exception as e:
                logger.warning(
                    f"Failed to parse reading from {peer_name} ({sensor_id}): {e}"
                )
                continue  # Continue listening even if one message is bad

    except Exception as e:
        logger.error(f"Unexpected error handling sensor {sensor_id} {peer_name}: {e}", exc_info=True)
    finally:
        writer.close()
        await writer.wait_closed()
        logger.info(f"Connection closed for sensor {sensor_id}")


async def start_tcp_server(
    host: str, 
    port: int, 
    storage: Any, 
    broadcaster: Optional[Any] = None,
) -> asyncio.AbstractServer:
    """Start the TCP ingest server listening on (host, port)."""
    global _storage, _broadcaster
    _storage = storage
    _broadcaster = broadcaster
    
    server = await asyncio.start_server(handle_sensor, host, port)
    logger.info(f"TCP ingest server listening on {host}:{port}")
    return server