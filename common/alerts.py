from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.request
from typing import Any, Optional


class AlertManager:
    def __init__(self, webhook_url: Optional[str], cooldown_seconds: float = 300.0) -> None:
        self.webhook_url = webhook_url
        self.cooldown_seconds = cooldown_seconds
        self._last_sent = 0.0
        self._last_sent_by_key: dict[str, float] = {}
        self.logger = logging.getLogger(__name__)

    def _should_send(self, key: str, level: str) -> bool:
        if not self.webhook_url:
            return False
        if level.upper() == "CRITICAL":
            return True
        last_sent = self._last_sent_by_key.get(key, 0.0)
        return (time.monotonic() - last_sent) >= self.cooldown_seconds

    async def send(
        self, message: str, level: str = "ERROR", context: Optional[dict[str, Any]] = None
    ) -> None:
        key = f"{level.upper()}:{message}"
        if not self._should_send(key, level):
            self.logger.debug("Alert suppressed by cooldown: %s", key)
            return
        payload = self._build_payload(message, level, context or {})
        try:
            await asyncio.to_thread(self._post_json, payload)
            now = time.monotonic()
            self._last_sent = now
            self._last_sent_by_key[key] = now
        except Exception as exc:
            self.logger.warning("Alert send failed: %s", exc)

    def _build_payload(
        self, message: str, level: str, context: dict[str, Any]
    ) -> dict[str, Any]:
        if self.webhook_url and "discord.com/api/webhooks" in self.webhook_url:
            content = f"[{level}] {message}"
            if context:
                context_str = json.dumps(context, ensure_ascii=True)
                content = f"{content} {context_str}"
            if len(content) > 1900:
                content = content[:1897] + "..."
            return {"content": content}
        return {"text": message, "level": level, "context": context}

    def _post_json(self, payload: dict[str, Any]) -> None:
        assert self.webhook_url is not None
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()


def send_discord_test_message(message: str = "Test message from bot") -> None:
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        raise RuntimeError("DISCORD_WEBHOOK_URL is not set")

    payload = {"content": message[:2000]}
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()
