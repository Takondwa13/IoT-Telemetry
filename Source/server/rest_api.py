"""REST API for the telemetry server.

Endpoints:
    GET    /sensors                       list registered sensors
    GET    /sensors/{id}/readings         historical readings  (?from=&to=)
    POST   /sensors                       register a new sensor
    DELETE /sensors/{id}                  remove a sensor

Content negotiation:
    Server-driven via the `Accept` header. Supported media types:
      application/json, application/xml, application/yaml.
    Delegates to server.serialization.

Sessions:
    A cookie identifies the client session — set on first response, read
    on subsequent requests.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Optional

from aiohttp import web

from server import serialization
from proto import telemetry_pb2

logger = logging.getLogger(__name__)

# Global storage - set by build_app
_storage: Optional[Any] = None
SESSION_COOKIE_NAME = "session_id"


async def list_sensors(request: web.Request) -> web.Response:
    """GET /sensors — list all registered sensors with latest readings."""
    if not _storage:
        return web.json_response({"error": "Storage not initialized"}, status=503)
    
    sensors = await _storage.list_sensors()
    
    logger.info(f"list_sensors: Found {len(sensors)} registered sensors")
    logger.info(f"Total sensors in storage: {len(_storage.sensors)}")
    logger.info(f"Total readings in storage: {sum(len(r) for r in _storage.readings.values())}")
    
    result = []
    for sensor in sensors:
        sensor_dict = serialization._protobuf_to_dict(sensor)
        
        # Add latest reading
        readings_list = _storage.readings.get(sensor.sensor_id, [])
        if readings_list:
            latest = readings_list[-1]
            sensor_dict["latest"] = {
                "value": latest.value,
                "reading_type": latest.reading_type,
                "timestamp": latest.timestamp.seconds
            }
        
        result.append(sensor_dict)
    
    # Fallback: if no sensors registered yet, show readings directly
    if not result and _storage.readings:
        logger.warning("No sensors registered, falling back to raw readings")
        for sensor_id, readings_list in _storage.readings.items():
            if readings_list:
                latest = readings_list[-1]
                result.append({
                    "sensor_id": sensor_id,
                    "sensor_type": latest.reading_type,
                    "latest": {
                        "value": latest.value,
                        "timestamp": latest.timestamp.seconds
                    }
                })
    
    media_type = serialization.negotiate(request)
    content = serialization.serialize(result, media_type)
    
    logger.info(f"Returning {len(result)} sensors/readings as {media_type}")
    
    response = web.Response(body=content, content_type=media_type)
    _set_session_cookie(request, response)
    return response


async def get_readings(request: web.Request) -> web.Response:
    """GET /sensors/{id}/readings — historical readings for a sensor."""
    if not _storage:
        return web.json_response({"error": "Storage not initialized"}, status=503)
    
    sensor_id = request.match_info.get("id")
    if not sensor_id:
        return web.json_response({"error": "Missing sensor ID"}, status=400)
    
    # Parse `from` and `to` query params
    from_ts = request.query.get("from")
    to_ts = request.query.get("to")
    
    try:
        from_ts = float(from_ts) if from_ts else None
        to_ts = float(to_ts) if to_ts else None
    except ValueError:
        return web.json_response(
            {"error": "Invalid from/to timestamp values"}, status=400
        )
    
    # === DEBUG LOGGING ===
    logger.info(f"get_readings called for sensor '{sensor_id}' (from={from_ts}, to={to_ts})")
    
    # Query storage
    readings = await _storage.get_readings(sensor_id, from_ts, to_ts)
    
    logger.info(f"Found {len(readings)} readings for sensor '{sensor_id}'")
    
    # Serialize via content negotiation
    media_type = serialization.negotiate(request)
    payload = [serialization._protobuf_to_dict(r) for r in readings]
    content = serialization.serialize(payload, media_type)
    
    response = web.Response(body=content, content_type=media_type)
    _set_session_cookie(request, response)
    return response


async def register_sensor(request: web.Request) -> web.Response:
    """POST /sensors — register a new sensor."""
    if not _storage:
        return web.json_response({"error": "Storage not initialized"}, status=503)
    
    try:
        # Parse body (respect Content-Type)
        content_type = request.content_type or "application/json"
        
        if "json" in content_type:
            data = await request.json()
        elif "yaml" in content_type or "text/yaml" in content_type:
            import yaml
            text = await request.text()
            data = yaml.safe_load(text)
        elif "xml" in content_type:
            # Simple XML parsing
            text = await request.text()
            data = _parse_simple_xml(text)
        else:
            data = await request.json()
        
        # Create sensor registration from data
        sensor = telemetry_pb2.SensorRegistration()
        sensor.sensor_id = data.get("sensor_id", "")
        sensor.sensor_type = data.get("sensor_type", "")
        
        if not sensor.sensor_id or not sensor.sensor_type:
            return web.json_response(
                {"error": "Missing sensor_id or sensor_type"}, status=400
            )
        
        # Create in storage
        await _storage.add_sensor(sensor)
        
        logger.info(f"Registered sensor {sensor.sensor_id} ({sensor.sensor_type})")
        
        # Return 201 Created with Location header
        response = web.json_response(
            serialization._protobuf_to_dict(sensor),
            status=201,
        )
        response.headers["Location"] = f"/sensors/{sensor.sensor_id}"
        _set_session_cookie(request, response)
        return response
        
    except Exception as e:
        logger.error(f"Error registering sensor: {e}")
        return web.json_response({"error": str(e)}, status=400)


async def delete_sensor(request: web.Request) -> web.Response:
    """DELETE /sensors/{id} — remove a sensor."""
    if not _storage:
        return web.json_response({"error": "Storage not initialized"}, status=503)
    
    sensor_id = request.match_info.get("id")
    if not sensor_id:
        return web.json_response({"error": "Missing sensor ID"}, status=400)
    
    # Delete from storage
    await _storage.remove_sensor(sensor_id)
    
    logger.info(f"Deleted sensor {sensor_id}")
    
    # Return 204 No Content
    response = web.Response(status=204)
    _set_session_cookie(request, response)
    return response


@web.middleware
async def session_cookie_middleware(request: web.Request, handler):
    """Set/read the session cookie on every request."""
    # Read existing cookie; assign one if missing
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_id:
        session_id = str(uuid.uuid4())
    
    # Attach session info to request for handlers
    request["session_id"] = session_id
    
    # Process the request
    response = await handler(request)
    
    # Ensure response sets the cookie when newly issued
    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_id,
        max_age=3600 * 24,  # 24 hours
        httponly=True,
    )
    
    return response


def build_app(storage: Any) -> web.Application:
    """Construct and return the aiohttp Application for the REST API."""
    global _storage
    _storage = storage
    
    # Create app with session middleware
    app = web.Application(middlewares=[session_cookie_middleware])
    
    # Add routes
    app.router.add_get("/sensors", list_sensors)
    app.router.add_get("/sensors/{id}/readings", get_readings)
    app.router.add_post("/sensors", register_sensor)
    app.router.add_delete("/sensors/{id}", delete_sensor)
    
    return app


def _set_session_cookie(request: web.Request, response: web.Response) -> None:
    """Helper to ensure session cookie is set on response."""
    if "session_id" in request:
        response.set_cookie(
            SESSION_COOKIE_NAME,
            request["session_id"],
            max_age=3600 * 24,
            httponly=True,
        )


def _parse_simple_xml(xml_str: str) -> dict:
    """Parse simple XML to dict (basic implementation)."""
    # Simple XML parsing for basic tags
    import re
    data = {}
    
    # Find all tag pairs
    pattern = r"<(\w+)>([^<]*)</\1>"
    matches = re.findall(pattern, xml_str)
    for tag, value in matches:
        data[tag] = value
    
    return data
