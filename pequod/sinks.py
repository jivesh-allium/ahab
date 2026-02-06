from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from .types import Alert


class AlertSink(ABC):
    @abstractmethod
    def send(self, alert: Alert) -> None:
        raise NotImplementedError


class ConsoleSink(AlertSink):
    def send(self, alert: Alert) -> None:
        print(alert.text)
        print("-" * 80)


class TelegramSink(AlertSink):
    def __init__(self, bot_token: str, chat_id: str, timeout_seconds: int) -> None:
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._timeout_seconds = timeout_seconds

    def send(self, alert: Alert) -> None:
        payload = {"chat_id": self._chat_id, "text": alert.text}
        self._post_json(
            f"https://api.telegram.org/bot{self._bot_token}/sendMessage",
            payload,
            timeout_seconds=self._timeout_seconds,
        )

    @staticmethod
    def _post_json(url: str, payload: Dict[str, object], timeout_seconds: int) -> None:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout_seconds):
            return


class DiscordSink(AlertSink):
    def __init__(self, webhook_url: str, timeout_seconds: int) -> None:
        self._webhook_url = webhook_url
        self._timeout_seconds = timeout_seconds

    def send(self, alert: Alert) -> None:
        payload = {"content": alert.text}
        self._post_json(self._webhook_url, payload)

    def _post_json(self, url: str, payload: Dict[str, object]) -> None:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self._timeout_seconds):
            return


class GenericWebhookSink(AlertSink):
    def __init__(self, webhook_url: str, timeout_seconds: int) -> None:
        self._webhook_url = webhook_url
        self._timeout_seconds = timeout_seconds

    def send(self, alert: Alert) -> None:
        payload = {
            "text": alert.text,
            "usd_value": alert.usd_value,
            "tx_id": alert.tx_id,
            "chain": alert.chain,
            "timestamp": alert.timestamp,
            "raw": alert.raw,
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=self._webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self._timeout_seconds):
            return


class MultiSink(AlertSink):
    def __init__(self, sinks: List[AlertSink]) -> None:
        self._sinks = sinks

    def send(self, alert: Alert) -> None:
        for sink in self._sinks:
            try:
                sink.send(alert)
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
                print(f"[sink-error] {sink.__class__.__name__}: {exc}", file=sys.stderr)


def build_sinks(
    timeout_seconds: int,
    telegram_bot_token: str,
    telegram_chat_id: str,
    discord_webhook_url: str,
    generic_webhook_url: str,
) -> MultiSink:
    sinks: List[AlertSink] = [ConsoleSink()]
    if telegram_bot_token and telegram_chat_id:
        sinks.append(TelegramSink(telegram_bot_token, telegram_chat_id, timeout_seconds))
    if discord_webhook_url:
        sinks.append(DiscordSink(discord_webhook_url, timeout_seconds))
    if generic_webhook_url:
        sinks.append(GenericWebhookSink(generic_webhook_url, timeout_seconds))
    return MultiSink(sinks)

