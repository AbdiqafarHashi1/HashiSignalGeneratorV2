from app.risk.instant_funded import InstantFundedRiskModel


def test_instant_risk_rules() -> None:
    model = InstantFundedRiskModel(leverage=2)
    assert model.can_trade() is True

    size = model.position_size(equity=100000, stop_loss_pct=1)
    assert size > 0

    model.update_after_trade(-1)
    model.update_after_trade(-1)
    model.update_after_trade(-1)
    assert model.cooldown_active is True
    assert model.can_trade() is False
