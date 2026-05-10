"""Entry point for the sensor simulator.

Run with:
    python -m client --config config/sensors.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import yaml

from client.simulator import SensorSimulator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    """Load the YAML config, spawn one task per sensor, run them all."""
    # Parse CLI args (path to YAML config)
    parser = argparse.ArgumentParser(
        description="IoT Telemetry Sensor Simulator"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/sensors.yaml",
        help="Path to sensor configuration YAML file",
    )
    args = parser.parse_args()
    
    # Load and validate config (server host/port, sensors list)
    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)
    
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)
    
    # Validate config structure
    if not config or "server" not in config or "sensors" not in config:
        logger.error("Invalid config: missing 'server' or 'sensors' sections")
        sys.exit(1)
    
    server_config = config["server"]
    host = server_config.get("host")
    port = server_config.get("port")
    
    if not host or not port:
        logger.error("Invalid config: server.host and server.port must be set")
        sys.exit(1)
    
    sensors_config = config["sensors"]
    if not sensors_config or len(sensors_config) == 0:
        logger.error("Invalid config: at least one sensor must be configured")
        sys.exit(1)
    
    logger.info(f"Loaded {len(sensors_config)} sensor(s) from {config_path}")
    logger.info(f"Connecting to server at {host}:{port}")
    
    # For each sensor entry, build a SensorSimulator and schedule .run()
    tasks = []
    for sensor_config in sensors_config:
        sensor_id = sensor_config.get("id")
        sensor_type = sensor_config.get("type")
        interval = sensor_config.get("interval_seconds")
        
        if not all([sensor_id, sensor_type, interval]):
            logger.error(
                f"Invalid sensor config: missing required fields "
                f"(id, type, interval_seconds)"
            )
            sys.exit(1)
        
        # Extract any additional per-sensor configuration
        extra_config = {
            k: v
            for k, v in sensor_config.items()
            if k not in ("id", "type", "interval_seconds")
        }
        
        simulator = SensorSimulator(
            sensor_id=sensor_id,
            sensor_type=sensor_type,
            interval_seconds=interval,
            host=host,
            port=port,
            **extra_config,
        )
        
        logger.info(
            f"Spawned simulator for sensor {sensor_id} "
            f"(type={sensor_type}, interval={interval}s)"
        )
        tasks.append(simulator.run())
    
    # Run all sensor simulators concurrently
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
