from app.risk.instant_funded import InstantFundedRiskModel


class RiskManager:
    def __init__(self, leverage: float):
        self.model = InstantFundedRiskModel(leverage=leverage)

    def can_trade(self) -> bool:
        return self.model.can_trade()

    def position_size(self, equity: float, stop_loss_pct: float) -> float:
        return self.model.position_size(equity=equity, stop_loss_pct=stop_loss_pct)

    def update_after_trade(self, pnl_pct: float) -> None:
        self.model.update_after_trade(pnl_pct)

    def risk_status(self) -> dict:
        return self.model.risk_status()
