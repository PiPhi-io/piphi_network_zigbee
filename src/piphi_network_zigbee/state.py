from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from piphi_runtime_kit_python import (
    build_local_event_record,
    build_runtime_identity,
    create_runtime_starter,
)

from .contract import CAPABILITIES, COMMANDS
from .mqtt_runtime import (
    ZigbeeMqttError,
    ZigbeeMqttSubscriber,
    ZigbeeMqttSubscription,
    normalize_state_payload,
    subscriptions_for_entry,
)
from .schemas import DeviceConfig
from .settings import DEFAULT_MQTT_BASE_TOPIC, DEFAULT_MQTT_SERVER, INTEGRATION_ID, INTEGRATION_NAME, INTEGRATION_VERSION
from .zigbee_devices import capability_metadata_from_exposes, infer_capabilities, normalize_device_id, normalize_exposes

starter = create_runtime_starter(
    integration_id=INTEGRATION_ID,
    integration_name=INTEGRATION_NAME,
    version=INTEGRATION_VERSION,
)
runtime = starter.runtime
registry = starter.registry
telemetry = starter.telemetry_client
config_sync = starter.config_sync

capabilities = CAPABILITIES
commands = COMMANDS
mqtt_subscribers: dict[str, ZigbeeMqttSubscriber] = {}
mqtt_subscription_errors: dict[str, str] = {}


def make_entry(config: DeviceConfig) -> dict[str, Any]:
    identity = build_runtime_identity(config, integration_id=INTEGRATION_ID)
    device_id = config.device_id or normalize_device_id(config.ieee_address or config.friendly_name)
    config_id = config.config_id or config.id
    capabilities = config.capabilities or infer_capabilities(config.definition)
    mqtt_base_topic = config.mqtt_base_topic or DEFAULT_MQTT_BASE_TOPIC
    exposes = config.exposes or normalize_exposes(config.definition)
    capability_metadata = config.capability_metadata or capability_metadata_from_exposes(exposes)
    return {
        **identity,
        "config_id": config_id,
        "device_id": device_id,
        "friendly_name": config.friendly_name,
        "ieee_address": config.ieee_address,
        "alias": config.alias,
        "model_id": config.model_id,
        "vendor": config.vendor,
        "description": config.description,
        "device_type": config.device_type,
        "mqtt_server": config.mqtt_server or DEFAULT_MQTT_SERVER,
        "mqtt_base_topic": mqtt_base_topic,
        "mqtt_topic": f"{mqtt_base_topic.strip('/')}/{config.friendly_name}",
        "sidecar_base_url": config.sidecar_base_url,
        "capabilities": capabilities,
        "capability_metadata": capability_metadata,
        "exposes": exposes,
        "definition": config.definition,
        "config": config.model_dump(),
    }


def append_runtime_event(
    event_type: str,
    device: dict[str, Any],
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event = build_local_event_record(
        event_type=event_type,
        device=device,
        payload=payload or {},
        source=INTEGRATION_ID,
        severity="info",
    )
    registry.append_event(event)
    return event


def get_entry_or_404(config_id: str) -> dict[str, Any]:
    entry = registry.get(config_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"unknown config_id={config_id}")
    return entry


async def apply_config(config: DeviceConfig) -> None:
    entry = make_entry(config)
    registry.set(config.id, entry)
    registry.update_state(
        config.id,
        {
            "connected": True,
            "availability": True,
            "friendly_name": config.friendly_name,
            "ieee_address": config.ieee_address,
            "alias": config.alias,
            "config_id": entry["config_id"],
            "mqtt_server": entry["mqtt_server"],
            "mqtt_topic": entry["mqtt_topic"],
        },
        device_id=entry["device_id"],
    )
    append_runtime_event(
        "zigbee.config.applied",
        entry,
        {
            "friendly_name": config.friendly_name,
            "ieee_address": config.ieee_address,
            "alias": config.alias,
        },
    )
    reconcile_mqtt_subscriptions()


async def remove_config(config_id: str) -> bool:
    entry = registry.remove(config_id)
    if entry is None:
        return False
    append_runtime_event(
        "zigbee.config.removed",
        entry,
        {"friendly_name": entry.get("friendly_name"), "alias": entry.get("alias")},
    )
    reconcile_mqtt_subscriptions()
    return True


def reconcile_mqtt_subscriptions() -> None:
    desired = _subscriptions_by_server()
    for server in list(mqtt_subscribers):
        if server not in desired:
            mqtt_subscribers.pop(server).stop()
            mqtt_subscription_errors.pop(server, None)
    for server, subscriptions in desired.items():
        existing = mqtt_subscribers.get(server)
        if existing and set(existing.topics) == {subscription.topic for subscription in subscriptions}:
            continue
        if existing:
            existing.stop()
        subscriber = ZigbeeMqttSubscriber(
            server=server,
            subscriptions=subscriptions,
            on_payload=_handle_mqtt_payload,
        )
        try:
            subscriber.start()
        except ZigbeeMqttError as exc:
            mqtt_subscription_errors[server] = str(exc)
        mqtt_subscribers[server] = subscriber


def stop_mqtt_subscriptions() -> None:
    for subscriber in mqtt_subscribers.values():
        subscriber.stop()
    mqtt_subscribers.clear()
    mqtt_subscription_errors.clear()


def mqtt_subscription_snapshot() -> dict[str, Any]:
    return {
        "running": any(subscriber.status in {"starting", "running"} for subscriber in mqtt_subscribers.values()),
        "servers": {
            server: {
                **subscriber.snapshot(),
                "error": mqtt_subscription_errors.get(server),
            }
            for server, subscriber in sorted(mqtt_subscribers.items())
        },
    }


def _subscriptions_by_server() -> dict[str, list[ZigbeeMqttSubscription]]:
    grouped: dict[str, list[ZigbeeMqttSubscription]] = {}
    for entry in registry.entries.values():
        server = str(entry.get("mqtt_server") or DEFAULT_MQTT_SERVER)
        subscriptions = subscriptions_for_entry(entry)
        if subscriptions:
            grouped.setdefault(server, []).extend(subscriptions)
    return grouped


def _handle_mqtt_payload(subscription: ZigbeeMqttSubscription, payload: dict[str, Any]) -> None:
    entry = registry.get(subscription.config_id)
    if entry is None:
        return
    state_update = normalize_state_payload(payload)
    current_snapshot = registry.state_snapshots.get(subscription.config_id)
    current_state = current_snapshot.get("state") if isinstance(current_snapshot, dict) else {}
    if not isinstance(current_state, dict):
        current_state = {}
    registry.update_state(
        subscription.config_id,
        {
            **current_state,
            **state_update,
            "mqtt_topic": entry.get("mqtt_topic"),
            "mqtt_server": entry.get("mqtt_server"),
        },
        device_id=subscription.device_id,
    )
    append_runtime_event(
        "device.state_changed",
        entry,
        {
            "topic": subscription.topic,
            "payload": payload,
            "state": state_update,
        },
    )
