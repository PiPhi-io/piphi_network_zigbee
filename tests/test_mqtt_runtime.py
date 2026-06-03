from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest

from piphi_network_zigbee import state as runtime_state
from piphi_network_zigbee.main import app
from piphi_network_zigbee.mqtt_runtime import (
    ZigbeeMqttClient,
    command_to_mqtt,
    decode_zigbee2mqtt_message,
    normalize_state_payload,
)
from piphi_network_zigbee.schemas import DeviceConfig


class FakeMqttModule:
    instances: list["FakeMqttClient"] = []

    class Client:
        def __init__(self, client_id: str) -> None:
            self.client_id = client_id
            self.on_connect = None
            self.on_message = None
            self.published: list[tuple[str, str]] = []
            self.subscriptions: list[str] = []
            self.username: str | None = None
            self.password: str | None = None
            FakeMqttModule.instances.append(self)

        def username_pw_set(self, username: str, password: str | None) -> None:
            self.username = username
            self.password = password

        def connect(self, host: str, port: int, keepalive: int) -> None:
            self.host = host
            self.port = port
            self.keepalive = keepalive
            if self.on_connect:
                self.on_connect(self, None, None, 0)

        def loop_start(self) -> None:
            pass

        def loop_stop(self) -> None:
            pass

        def disconnect(self) -> None:
            pass

        def publish(self, topic: str, payload: str):
            self.published.append((topic, payload))
            return SimpleNamespace(rc=0, wait_for_publish=lambda timeout=None: None)

        def subscribe(self, topic: str) -> None:
            self.subscriptions.append(topic)
            payload = {"state": "ON", "brightness": 127, "temperature": 21.4}
            self.on_message(self, None, SimpleNamespace(payload=json.dumps(payload).encode("utf-8")))


FakeMqttClient = FakeMqttModule.Client


def test_command_to_mqtt_builds_zigbee2mqtt_payloads() -> None:
    entry = {
        "mqtt_topic": "zigbee2mqtt/kitchen_light",
        "capabilities": ["state", "brightness_percent"],
    }

    assert command_to_mqtt(entry, "set_state", {"on": True}) == {
        "topic": "zigbee2mqtt/kitchen_light/set",
        "payload": {"state": "ON"},
    }
    assert command_to_mqtt(entry, "set_brightness", {"brightness_percent": 50}) == {
        "topic": "zigbee2mqtt/kitchen_light/set",
        "payload": {"brightness": 127},
    }
    assert command_to_mqtt(entry, "refresh", {}) == {
        "topic": "zigbee2mqtt/kitchen_light/get",
        "payload": {"brightness": "", "state": ""},
    }


def test_refresh_command_uses_readable_expose_properties() -> None:
    entry = {
        "mqtt_topic": "zigbee2mqtt/environment_sensor",
        "capabilities": ["temperature_c", "humidity_percent"],
        "exposes": [
            {"property": "temperature", "access": {"published": True, "set": False, "get": True}},
            {"property": "humidity", "access": {"published": True, "set": False, "get": True}},
            {"property": "linkquality", "access": {"published": True, "set": False, "get": False}},
        ],
    }

    assert command_to_mqtt(entry, "refresh", {}) == {
        "topic": "zigbee2mqtt/environment_sensor/get",
        "payload": {"humidity": "", "temperature": ""},
    }


def test_normalize_state_payload_maps_zigbee2mqtt_state() -> None:
    state = normalize_state_payload(
        {
            "state": "ON",
            "brightness": 254,
            "contact": False,
            "temperature": 19.5,
        }
    )

    assert state["connected"] is True
    assert state["state"] is True
    assert state["brightness_percent"] == 100
    assert state["contact_open"] is False
    assert state["temperature_c"] == 19.5


def test_decode_availability_message() -> None:
    assert decode_zigbee2mqtt_message(b"online", kind="availability") == {"availability": "online"}
    assert decode_zigbee2mqtt_message(b'{"state":"offline"}', kind="availability") == {"availability": "offline"}


@pytest.mark.anyio
async def test_mqtt_client_publishes_and_reads_json() -> None:
    FakeMqttModule.instances.clear()
    client = ZigbeeMqttClient(
        server="mqtt://user:pass@broker.local:1884",
        mqtt_module=FakeMqttModule,
    )

    publish_result = await client.publish_json("zigbee2mqtt/light/set", {"state": "ON"})
    read_result = await client.read_json_topic("zigbee2mqtt/light")

    assert publish_result["ok"] is True
    assert read_result["payload"]["state"] == "ON"
    publish_client = FakeMqttModule.instances[0]
    assert publish_client.host == "broker.local"
    assert publish_client.port == 1884
    assert publish_client.username == "user"
    assert publish_client.password == "pass"
    assert publish_client.published == [("zigbee2mqtt/light/set", '{"state":"ON"}')]


@pytest.mark.anyio
async def test_command_route_publishes_zigbee2mqtt_dry_run() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        config_response = await client.post(
            "/config",
            json={
                "id": "test-kitchen-light",
                "friendly_name": "kitchen_light",
                "mqtt_server": "mqtt://broker.local:1883",
                "mqtt_base_topic": "zigbee2mqtt",
                "capabilities": ["state", "brightness_percent"],
            },
        )
        command_response = await client.post(
            "/command",
            json={
                "command": "set_state",
                "target": {
                    "config_id": "test-kitchen-light",
                    "device_id": "test-kitchen-light",
                },
                "params": {"on": True},
                "dry_run": True,
            },
        )

    assert config_response.status_code == 200
    assert command_response.status_code == 200
    command = command_response.json()
    assert command["ok"] is True
    assert command["mqtt"]["status"] == "planned"
    assert command["mqtt"]["topic"] == "zigbee2mqtt/kitchen_light/set"
    assert command["mqtt"]["payload"] == {"state": "ON"}

    await runtime_state.remove_config("test-kitchen-light")


@pytest.mark.anyio
async def test_config_sync_accepts_core_snapshot_payloads() -> None:
    config_id = "snapshot-motion-sensor"
    await runtime_state.remove_config(config_id)
    runtime_state.runtime.set_current_generation(None)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/config/sync",
            json={
                "container_id": "runtime-1",
                "generation": 12345,
                "configs": [
                    {
                        "id": config_id,
                        "container_id": "runtime-1",
                        "device_id": "0xb40e060fffe7068b",
                        "friendly_name": "Motion Sensor 1 In Bedroom",
                        "ieee_address": "0xb40e060fffe7068b",
                        "alias": "Third Reality Wireless motion sensor",
                        "mqtt_server": "mqtt://broker.local:1883",
                        "mqtt_base_topic": "zigbee2mqtt",
                        "mqtt_topic": "zigbee2mqtt/0xb40e060fffe7068b",
                        "capabilities": ["occupancy"],
                    },
                ],
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "synced"
    assert payload["generation"] == 12345
    assert payload["applied"] == [config_id]
    assert runtime_state.config_sync.get_current_generation() == 12345
    assert runtime_state.registry.get(config_id)["alias"] == "Third Reality Wireless motion sensor"
    assert runtime_state.registry.get(config_id)["device_id"] == "0xb40e060fffe7068b"
    assert runtime_state.registry.get(config_id)["mqtt_topic"] == "zigbee2mqtt/0xb40e060fffe7068b"

    await runtime_state.remove_config(config_id)
    runtime_state.runtime.set_current_generation(None)


@pytest.mark.anyio
async def test_apply_config_starts_subscription_and_ingests_state(monkeypatch) -> None:
    started: list["FakeSubscriber"] = []
    telemetry_deliveries: list[dict] = []

    class FakeSubscriber:
        def __init__(self, *, server, subscriptions, on_payload):
            self.server = server
            self.subscriptions = subscriptions
            self.on_payload = on_payload
            self.status = "stopped"
            self.message = None
            self.started_at = None
            started.append(self)

        @property
        def topics(self):
            return [subscription.topic for subscription in self.subscriptions]

        def start(self):
            self.status = "running"
            for subscription in self.subscriptions:
                if subscription.kind == "state":
                    self.on_payload(subscription, {"state": "ON", "brightness": 254})
                if subscription.kind == "availability":
                    self.on_payload(subscription, {"availability": "online"})

        def stop(self):
            self.status = "stopped"

        def snapshot(self):
            return {"server": self.server, "status": self.status, "topics": self.topics}

    monkeypatch.setattr(runtime_state, "ZigbeeMqttSubscriber", FakeSubscriber)
    monkeypatch.setattr(
        runtime_state,
        "schedule_telemetry_delivery",
        lambda **kwargs: telemetry_deliveries.append(kwargs),
    )
    await runtime_state.remove_config("test-kitchen-light")
    config_id = "subscribed-kitchen-light"
    await runtime_state.apply_config(
        DeviceConfig(
            id=config_id,
            device_id="0xb40e060fffe7068b",
            friendly_name="Motion Sensor 1 In Bedroom",
            mqtt_server="mqtt://broker.local:1883",
            mqtt_topic="zigbee2mqtt/0xb40e060fffe7068b",
            capabilities=["state", "brightness_percent"],
        )
    )

    snapshot = runtime_state.registry.state_snapshots[config_id]
    assert "zigbee2mqtt/0xb40e060fffe7068b" in started[0].topics
    assert "zigbee2mqtt/0xb40e060fffe7068b/availability" in started[0].topics
    assert snapshot["state"]["state"] is True
    assert snapshot["state"]["brightness_percent"] == 100
    assert snapshot["state"]["availability"] is True
    assert runtime_state.mqtt_subscription_snapshot()["running"] is True
    assert [delivery["device_id"] for delivery in telemetry_deliveries] == [
        "0xb40e060fffe7068b",
        "0xb40e060fffe7068b",
    ]
    assert telemetry_deliveries[0]["metrics"]["state"] is True
    assert telemetry_deliveries[0]["metrics"]["brightness_percent"] == 100
    assert telemetry_deliveries[1]["metrics"]["availability"] is True

    runtime_state.stop_mqtt_subscriptions()
    await runtime_state.remove_config(config_id)
