from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.entities import TelegramLog

logger = logging.getLogger(__name__)


class TelegramService:
    def __init__(self, bot_token: str | None = None, chat_id: str | None = None):
        self.bot_token = bot_token if bot_token is not None else settings.telegram_bot_token
        self.chat_id = chat_id if chat_id is not None else settings.telegram_chat_id
        enabled_cfg = str(getattr(settings, 'telegram_enabled', '')).strip().lower()
        self.enabled = (enabled_cfg in {'1', 'true', 'yes'}) if enabled_cfg else bool(self.bot_token and self.chat_id)
        self.status_interval_min = int(getattr(settings, 'telegram_status_interval_min', 60) or 60)
        self.no_trade_interval_min = int(getattr(settings, 'telegram_no_trade_interval_min', 30) or 30)
        self._last_status_at: datetime | None = None
        self._last_no_trade_at: datetime | None = None
        self._last_blocker: str | None = None
        reason = 'ready' if self.enabled else 'missing token/chat_id or TELEGRAM_ENABLED=false'
        logger.info('Telegram %s (%s)', 'enabled' if self.enabled else 'disabled', reason)

    async def _log(self, db: AsyncSession, message_type: str, body: str, status: str) -> None:
        db.add(TelegramLog(message_type=message_type, body=body, status=status))
        await db.commit()

    async def _send_http(self, body: str) -> None:
        if not self.bot_token or not self.chat_id:
            raise RuntimeError('missing telegram bot token/chat id')
        url = f'https://api.telegram.org/bot{self.bot_token}/sendMessage'
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.post(url, json={'chat_id': self.chat_id, 'text': body, 'disable_web_page_preview': True})
            resp.raise_for_status()

    async def _send(self, db: AsyncSession, message_type: str, body: str) -> dict:
        if not self.enabled:
            await self._log(db, message_type, body, 'DISABLED')
            return {'status': 'disabled', 'message': body}
        await self._log(db, message_type, body, 'QUEUED')

        async def _runner() -> None:
            try:
                await self._send_http(body)
            except Exception as exc:  # noqa: BLE001
                logger.warning('telegram send failed: %s', exc)

        asyncio.create_task(_runner())
        return {'status': 'queued', 'message': body}

    async def send_signal_message(self, db: AsyncSession, body: str) -> dict:
        return await self._send(db, 'signal', body)

    async def send_trade_alert(self, db: AsyncSession, body: str) -> dict:
        return await self._send(db, 'trade', body)

    async def send_risk_alert(self, db: AsyncSession, body: str) -> dict:
        return await self._send(db, 'risk', body)


    def notify(self, body: str) -> None:
        if not self.enabled:
            return
        async def _runner() -> None:
            try:
                await self._send_http(body)
            except Exception as exc:  # noqa: BLE001
                logger.warning('telegram send failed: %s', exc)
        asyncio.create_task(_runner())
    def should_send_status(self, now: datetime | None = None) -> bool:
        if not self.enabled:
            return False
        current = now or datetime.now(timezone.utc)
        if not self._last_status_at:
            self._last_status_at = current
            return True
        if (current - self._last_status_at).total_seconds() >= self.status_interval_min * 60:
            self._last_status_at = current
            return True
        return False

    def should_send_no_trade(self, blocker: str | None, now: datetime | None = None) -> bool:
        if not self.enabled:
            return False
        current = now or datetime.now(timezone.utc)
        changed = blocker and blocker != self._last_blocker
        threshold_hit = (not self._last_no_trade_at) or ((current - self._last_no_trade_at).total_seconds() >= self.no_trade_interval_min * 60)
        if changed or threshold_hit:
            self._last_no_trade_at = current
            self._last_blocker = blocker
            return True
        return False
