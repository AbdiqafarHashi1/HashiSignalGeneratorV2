#!/usr/bin/env node

function roundToTick(price, tick) {
  if (!Number.isFinite(tick) || tick <= 0) return Number.isFinite(price) ? price : 0;
  return Math.round((Number.isFinite(price) ? price : 0) / tick) * tick;
}

function roundToStep(qty, step) {
  if (!Number.isFinite(step) || step <= 0) return Number.isFinite(qty) ? qty : 0;
  return Math.floor((Number.isFinite(qty) ? qty : 0) / step) * step;
}

function calcFee(notional, feeModel, role) {
  const safeNotional = Number.isFinite(notional) ? Math.max(0, notional) : 0;
  const bps = role === "maker" ? feeModel.maker_bps : feeModel.taker_bps;
  return safeNotional * (Math.max(0, bps) / 10000);
}

function validateMinNotional(qty, price, minNotional) {
  return qty * price >= minNotional;
}

function assert(name, condition) {
  if (condition) {
    console.log(`PASS: ${name}`);
    return true;
  }
  console.error(`FAIL: ${name}`);
  return false;
}

let passed = 0;
let total = 0;
const run = (name, condition) => {
  total += 1;
  if (assert(name, condition)) passed += 1;
};

run("tick rounding", Math.abs(roundToTick(123.456, 0.01) - 123.46) < 1e-9);
run("qty rounding", roundToStep(1.23456, 0.001) === 1.234);
run("min notional validation", validateMinNotional(0.01, 1000, 5) === true);
run("min notional reject", validateMinNotional(0.001, 1000, 5) === false);
run("fee calc taker", Math.abs(calcFee(1000, { maker_bps: 2, taker_bps: 6 }, "taker") - 0.6) < 1e-9);
run("fee calc maker", Math.abs(calcFee(1000, { maker_bps: 2, taker_bps: 6 }, "maker") - 0.2) < 1e-9);

console.log(`\nExecution sanity: ${passed}/${total} passed`);
process.exit(passed === total ? 0 : 1);
