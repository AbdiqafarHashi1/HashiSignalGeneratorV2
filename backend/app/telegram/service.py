from __future__ import annotations

import asyncio
import logging
import time
from collections import deque

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.models.entities import TelegramLog

logger = logging.getLogger(__name__)


class TelegramService:
    def __init__(self, bot_token: str | None = None, chat_id: str | None = None):
        self.bot_token = bot_token if bot_token is not None else settings.telegram_bot_token
        self.chat_id = chat_id if chat_id is not None else settings.telegram_chat_id
        configured = bool(self.bot_token and self.chat_id)
        self.enabled = configured if settings.telegram_enabled is None else bool(settings.telegram_enabled and configured)
        self.status_interval_s = max(60, int(settings.telegram_status_interval_min) * 60)
        self.no_trade_interval_s = max(60, int(settings.telegram_no_trade_interval_min) * 60)
        self._queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=1000)
        self._worker: asyncio.Task | None = None
        self._session_factory: async_sessionmaker | None = None
        self._last_status_sent_at = 0.0
        self._last_no_trade_sent_at = 0.0
        self._last_no_trade_key: str | None = None
        self._failure_ring: deque[float] = deque(maxlen=100)
        logger.info(
            'Telegram %s (%s)',
            'enabled' if self.enabled else 'disabled',
            'configured' if configured else 'missing token/chat id',
        )

    def configure(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._worker_loop())

    async def _log(self, db: AsyncSession, message_type: str, body: str, status: str) -> None:
        db.add(TelegramLog(message_type=message_type, body=body, status=status))
        await db.commit()

    async def _send_http(self, body: str) -> tuple[bool, str]:
        if not self.enabled:
            return False, 'disabled'
        url = f'https://api.telegram.org/bot{self.bot_token}/sendMessage'
        payload = {'chat_id': self.chat_id, 'text': body, 'parse_mode': 'HTML', 'disable_web_page_preview': True}
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(url, json=payload)
            if resp.is_success:
                return True, 'SENT'
            return False, f'HTTP_{resp.status_code}'
        except Exception as exc:
            return False, f'ERR:{exc.__class__.__name__}'

    async def _worker_loop(self) -> None:
        while True:
            message_type, body = await self._queue.get()
            status = 'SKIPPED'
            if self.enabled:
                ok, status = await self._send_http(body)
                if not ok:
                    now = time.time()
                    self._failure_ring.append(now)
                    if not self._failure_ring or now - self._failure_ring[-1] > 30:
                        logger.warning('Telegram send failed: %s', status)
            if self._session_factory:
                async with self._session_factory() as db:
                    await self._log(db, message_type, body, status)
            self._queue.task_done()

    async def enqueue(self, message_type: str, body: str) -> dict:
        if self._worker is None and self._session_factory:
            self.configure(self._session_factory)
        try:
            self._queue.put_nowait((message_type, body))
            return {'status': 'queued', 'message': body}
        except asyncio.QueueFull:
            logger.warning('Telegram queue full, dropping message type=%s', message_type)
            return {'status': 'dropped', 'message': body}

    async def send_signal_message(self, db: AsyncSession, body: str) -> dict:
        await self._log(db, 'signal', body, 'QUEUED')
        return await self.enqueue('signal', body)

    async def send_trade_alert(self, db: AsyncSession, body: str) -> dict:
        await self._log(db, 'trade', body, 'QUEUED')
        return await self.enqueue('trade', body)

    async def send_risk_alert(self, db: AsyncSession, body: str) -> dict:
        await self._log(db, 'risk', body, 'QUEUED')
        return await self.enqueue('risk', body)

    def should_send_status(self) -> bool:
        now = time.time()
        if now - self._last_status_sent_at >= self.status_interval_s:
            self._last_status_sent_at = now
            return True
        return False

    def should_send_no_trade(self, blocker_key: str) -> bool:
        now = time.time()
        changed = blocker_key != self._last_no_trade_key
        if changed or (now - self._last_no_trade_sent_at >= self.no_trade_interval_s):
            self._last_no_trade_sent_at = now
            self._last_no_trade_key = blocker_key
            return True
        return False
