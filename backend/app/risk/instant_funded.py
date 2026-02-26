from dataclasses import dataclass, field


@dataclass
class InstantFundedRiskModel:
    monthly_target_pct: float = 6.0
    max_global_drawdown_pct: float = 6.0
    max_daily_drawdown_pct: float = 3.0
    max_trades_per_day: int = 10
    base_risk_pct: float = 0.15
    max_risk_pct: float = 0.20
    leverage: float = 1.0
    consecutive_losses: int = 0
    trades_today: int = 0
    daily_drawdown_pct: float = 0.0
    global_drawdown_pct: float = 0.0
    monthly_progress_pct: float = 0.0
    cooldown_active: bool = False
    last_result: str | None = None
    history: list[dict] = field(default_factory=list)

    def can_trade(self) -> bool:
        if self.cooldown_active:
            return False
        if self.trades_today >= self.max_trades_per_day:
            return False
        if self.daily_drawdown_pct >= self.max_daily_drawdown_pct:
            return False
        if self.global_drawdown_pct >= self.max_global_drawdown_pct:
            return False
        return True

    def position_size(self, equity: float, stop_loss_pct: float) -> float:
        risk_pct = min(self.base_risk_pct + (self.monthly_progress_pct / 1000), self.max_risk_pct)
        if stop_loss_pct <= 0:
            return 0.0
        return round((equity * (risk_pct / 100) * self.leverage) / stop_loss_pct, 8)

    def update_after_trade(self, pnl_pct: float) -> None:
        self.trades_today += 1
        self.monthly_progress_pct += pnl_pct
        self.last_result = 'WIN' if pnl_pct > 0 else 'LOSS'
        if pnl_pct < 0:
            self.consecutive_losses += 1
            self.daily_drawdown_pct += abs(pnl_pct)
            self.global_drawdown_pct += abs(pnl_pct)
        else:
            self.consecutive_losses = 0
        self.cooldown_active = self.consecutive_losses >= 3
        self.history.append({'pnl_pct': pnl_pct, 'trades_today': self.trades_today})

    def risk_status(self) -> dict:
        return {
            'monthly_target_pct': self.monthly_target_pct,
            'monthly_progress_pct': round(self.monthly_progress_pct, 4),
            'max_global_drawdown_pct': self.max_global_drawdown_pct,
            'global_drawdown_pct': round(self.global_drawdown_pct, 4),
            'max_daily_drawdown_pct': self.max_daily_drawdown_pct,
            'daily_drawdown_pct': round(self.daily_drawdown_pct, 4),
            'max_trades_per_day': self.max_trades_per_day,
            'trades_today': self.trades_today,
            'base_risk_pct': self.base_risk_pct,
            'max_risk_pct': self.max_risk_pct,
            'cooldown_active': self.cooldown_active,
            'can_trade': self.can_trade(),
            'leverage': self.leverage,
            'last_result': self.last_result,
        }
