from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.entities import TelegramLog


class TelegramService:
    def __init__(self, bot_token: str | None = None, chat_id: str | None = None):
        self.bot_token = bot_token if bot_token is not None else settings.telegram_bot_token
        self.chat_id = chat_id if chat_id is not None else settings.telegram_chat_id

    async def _log(self, db: AsyncSession, message_type: str, body: str, status: str) -> None:
        db.add(TelegramLog(message_type=message_type, body=body, status=status))
        await db.commit()

    async def _send(self, db: AsyncSession, message_type: str, body: str) -> dict:
        if not self.bot_token or not self.chat_id:
            await self._log(db, message_type, body, 'MANUAL_FALLBACK')
            return {'status': 'manual_fallback', 'message': body}
        await self._log(db, message_type, body, 'QUEUED')
        return {'status': 'queued', 'message': body}

    async def send_signal_message(self, db: AsyncSession, body: str) -> dict:
        return await self._send(db, 'signal', body)

    async def send_trade_alert(self, db: AsyncSession, body: str) -> dict:
        return await self._send(db, 'trade', body)

    async def send_risk_alert(self, db: AsyncSession, body: str) -> dict:
        return await self._send(db, 'risk', body)
