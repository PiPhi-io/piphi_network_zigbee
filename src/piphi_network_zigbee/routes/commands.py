from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from piphi_runtime_kit_python import build_event_ingest_response

from ..mqtt_runtime import ZigbeeMqttClient, ZigbeeMqttError, command_to_mqtt
from ..state import append_runtime_event, commands, registry

router = APIRouter(tags=["commands"])


@router.post("/command")
async def command(payload: dict[str, Any]):
    command_name = str(payload.get("command") or payload.get("capability_id") or "").strip()
    if not command_name:
        raise HTTPException(status_code=400, detail="Missing command")
    if command_name not in commands:
        raise HTTPException(status_code=400, detail=f"Unsupported command: {command_name}")

    target = payload.get("target") if isinstance(payload.get("target"), dict) else {}
    device_id = str(payload.get("device_id") or target.get("device_id") or "demo-device")
    config_id = str(payload.get("config_id") or target.get("config_id") or device_id)
    requirements = payload.get("capability_requirements")
    requested_capabilities = [
        str(item).strip()
        for item in ([payload.get("capability")] + (requirements if isinstance(requirements, list) else []))
        if str(item or "").strip()
    ]
    unsupported_capability = next(
        (
            capability
            for capability in requested_capabilities
            if capability not in {"device.refresh", f"action.{command_name}", command_name}
        ),
        None,
    )
    if unsupported_capability:
        raise HTTPException(
            status_code=400,
            detail={
                "ok": False,
                "error": "unsupported_capability",
                "message": f"This runtime does not support capability {unsupported_capability}",
            },
        )
    params = payload.get("params") or payload.get("args") or {}
    if not isinstance(params, dict):
        raise HTTPException(status_code=400, detail="Command params must be an object.")
    entry = registry.get(config_id)
    if entry is None:
        entry = {
            "device_id": device_id,
            "config_id": config_id,
            "mqtt_server": "mqtt://127.0.0.1:1883",
            "mqtt_topic": f"zigbee2mqtt/{device_id}",
            "capabilities": ["refresh"],
        }
        dry_run = True
    else:
        dry_run = bool(payload.get("dry_run") or params.get("dry_run"))
    dry_run = dry_run or bool(params.get("dry_run"))
    try:
        mqtt_command = command_to_mqtt(entry, command_name, params)
        mqtt_result = await ZigbeeMqttClient(
            server=str(entry.get("mqtt_server") or "mqtt://127.0.0.1:1883"),
        ).publish_json(
            mqtt_command["topic"],
            mqtt_command["payload"],
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ZigbeeMqttError as exc:
        if command_name == "refresh":
            mqtt_result = {
                "ok": False,
                "status": "unreachable",
                "topic": mqtt_command["topic"],
                "payload": mqtt_command["payload"],
                "message": str(exc),
            }
        else:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
    event = append_runtime_event(
        "zigbee.command.published",
        entry,
        {
            "command": command_name,
            "device_id": device_id,
            "entity_id": payload.get("entity_id"),
            "args": params,
            "target": target,
            "mqtt": mqtt_result,
        },
    )
    response = build_event_ingest_response(event)
    response_payload = response.model_dump() if hasattr(response, "model_dump") else dict(response)
    return {
        **response_payload,
        "ok": True,
        "command": command_name,
        "contract_version": payload.get("contract_version"),
        "device_id": device_id,
        "config_id": config_id,
        "target": target,
        "params": params,
        "mqtt": mqtt_result,
    }
