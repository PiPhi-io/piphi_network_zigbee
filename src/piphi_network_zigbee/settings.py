from __future__ import annotations

import os

INTEGRATION_ID = "piphi-network-zigbee"
INTEGRATION_NAME = "PiPhi Network Zigbee"
INTEGRATION_VERSION = "0.1.4"
PROJECT_KIND = "integration"
PROJECT_PRESET = "protocol-bridge"
PROJECT_DOMAIN = "bridge"
DEFAULT_PORT = 8730
DEFAULT_SIDECAR_BASE_URL = os.getenv("ZIGBEE2MQTT_SIDECAR_URL", "http://127.0.0.1:8720").rstrip("/")
DEFAULT_MQTT_SERVER = os.getenv("MQTT_SERVER", "mqtt://127.0.0.1:1883").strip() or "mqtt://127.0.0.1:1883"
DEFAULT_MQTT_BASE_TOPIC = os.getenv("MQTT_BASE_TOPIC", "zigbee2mqtt").strip().strip("/") or "zigbee2mqtt"


def runtime_port() -> int:
    raw_port = os.getenv("PORT", str(DEFAULT_PORT))
    try:
        return int(raw_port)
    except ValueError:
        return DEFAULT_PORT
