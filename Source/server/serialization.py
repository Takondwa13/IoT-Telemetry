"""Content negotiation for the REST API.

Maps the `Accept` header on a request to a serializer for the response.
Supported media types:
    application/json
    application/xml
    application/yaml   (also accepts text/yaml)

Falls back to JSON when no supported type matches.
"""
from __future__ import annotations

import json
import logging
from typing import Any

import yaml
from aiohttp import web

logger = logging.getLogger(__name__)


def negotiate(request: web.Request) -> str:
    """Return the chosen response media type for `request`.
    JSON is now the DEFAULT format.
    """
    accept_header = request.headers.get("Accept", "").lower()

    # Check for explicit preferences (in order of priority)
    if "application/xml" in accept_header:
        return "application/xml"
    elif "application/yaml" in accept_header or "text/yaml" in accept_header:
        return "application/yaml"
    elif "application/json" in accept_header or "*/*" in accept_header or accept_header == "":
        return "application/json"
    
    # Default to JSON (this is the main change)
    return "application/json"


def serialize(payload: Any, media_type: str) -> bytes:
    """Serialize `payload` (a dict or list of dicts) into bytes."""
    if not media_type or media_type == "*/*":
        media_type = "application/json"
        
    if media_type == "application/json":
        # Convert protobuf messages to dicts if needed
        if isinstance(payload, list):
            data = [_protobuf_to_dict(item) for item in payload]
        else:
            data = _protobuf_to_dict(payload) if payload else {}
        return json.dumps(data).encode()
    
    elif media_type == "application/yaml":
        # Convert protobuf messages to dicts if needed
        if isinstance(payload, list):
            data = [_protobuf_to_dict(item) for item in payload]
        else:
            data = _protobuf_to_dict(payload) if payload else {}
        return yaml.dump(data).encode()
    
    elif media_type == "application/xml":
        # Simple XML serialization
        if isinstance(payload, list):
            xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n<root>\n'
            for item in payload:
                xml_str += _dict_to_xml(_protobuf_to_dict(item), "item", indent="  ")
            xml_str += '</root>'
        else:
            xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n'
            xml_str += _dict_to_xml(_protobuf_to_dict(payload) if payload else {}, "root")
        return xml_str.encode()
    
    # Default to JSON
    if isinstance(payload, list):
        data = [_protobuf_to_dict(item) for item in payload]
    else:
        data = _protobuf_to_dict(payload) if payload else {}
    return json.dumps(data).encode()


def _protobuf_to_dict(obj: Any) -> dict:
    """Convert a protobuf message to a dictionary."""
    if hasattr(obj, "ListFields"):
        # It's a protobuf message
        result = {}
        for field, value in obj.ListFields():
            field_name = field.name
            if field.message_type and field.label != field.LABEL_REPEATED:
                # Nested message
                result[field_name] = _protobuf_to_dict(value)
            elif field.message_type and field.label == field.LABEL_REPEATED:
                # Repeated field
                result[field_name] = [_protobuf_to_dict(v) for v in value]
            else:
                # Scalar field
                result[field_name] = value
        return result
    elif isinstance(obj, dict):
        return obj
    else:
        return {"value": str(obj)}


def _dict_to_xml(data: dict, tag: str = "item", indent: str = "") -> str:
    """Convert a dict to simple XML."""
    if not isinstance(data, dict):
        return f"{indent}<{tag}>{_escape_xml(str(data))}</{tag}>\n"
    
    xml = f"{indent}<{tag}>\n"
    for key, value in data.items():
        safe_key = key.replace("-", "_")
        if isinstance(value, dict):
            xml += _dict_to_xml(value, safe_key, indent + "  ")
        elif isinstance(value, list):
            for item in value:
                xml += _dict_to_xml({"_": item} if not isinstance(item, dict) else item, safe_key, indent + "  ")
        else:
            xml += f"{indent}  <{safe_key}>{_escape_xml(str(value))}</{safe_key}>\n"
    xml += f"{indent}</{tag}>\n"
    return xml


def _escape_xml(text: str) -> str:
    """Escape XML special characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )
