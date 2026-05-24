from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from ..contract import ENDPOINTS, REQUIRED_ENDPOINTS
from ..mqtt_runtime import ZigbeeMqttClient, ZigbeeMqttError, normalize_state_payload
from ..settings import (
    INTEGRATION_ID,
    INTEGRATION_NAME,
    INTEGRATION_VERSION,
    PROJECT_DOMAIN,
    PROJECT_KIND,
    PROJECT_PRESET,
)
from ..state import mqtt_subscription_snapshot, registry

router = APIRouter(tags=["runtime"])


@router.get("/state")
async def state(refresh: bool = False, dry_run: bool = False) -> dict[str, Any]:
    if refresh:
        await _refresh_mqtt_state(dry_run=dry_run)
    return {
        "summary": {
            "active_config_count": len(registry.ids()),
            "recent_event_count": len(registry.recent_events),
        },
        "entries": registry.entries,
        "state_snapshots": registry.state_snapshots,
        "mqtt_subscriptions": mqtt_subscription_snapshot(),
    }


async def _refresh_mqtt_state(*, dry_run: bool = False) -> None:
    for config_id, entry in list(registry.entries.items()):
        topic = str(entry.get("mqtt_topic") or "").strip()
        server = str(entry.get("mqtt_server") or "mqtt://127.0.0.1:1883")
        if not topic:
            continue
        try:
            result = await ZigbeeMqttClient(server=server).read_json_topic(topic, dry_run=dry_run)
        except ZigbeeMqttError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        payload = result.get("payload") if isinstance(result, dict) else {}
        if isinstance(payload, dict):
            registry.update_state(
                config_id,
                {
                    **normalize_state_payload(payload),
                    "mqtt_topic": topic,
                    "mqtt_server": server,
                },
                device_id=str(entry.get("device_id") or config_id),
            )


@router.get("/contract")
async def contract() -> dict[str, Any]:
    return {
        "integration_id": INTEGRATION_ID,
        "name": INTEGRATION_NAME,
        "version": INTEGRATION_VERSION,
        "kind": PROJECT_KIND,
        "preset": PROJECT_PRESET,
        "domain": PROJECT_DOMAIN,
        "endpoints": ENDPOINTS,
        "required": REQUIRED_ENDPOINTS,
    }
