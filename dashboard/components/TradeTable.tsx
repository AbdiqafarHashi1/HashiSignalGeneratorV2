'use client';

import { useMemo, useState } from 'react';

const text = (value: any) => (value === null || value === undefined || value === '' ? '-' : String(value));
const fmtNum = (value: any, digits = 2) => {
  if (value === null || value === undefined || value === '') return '-';
  const n = Number(value);
  return Number.isFinite(n) ? n.toFixed(digits) : String(value);
};
const fmtTs = (primary: any, fallback?: any) => {
  const value = primary ?? fallback;
  if (!value) return '-';
  const ts = new Date(value);
  if (Number.isNaN(ts.getTime())) return String(value);
  return ts.toISOString().replace('T', ' ').replace('Z', ' UTC');
};

const sideInfo = (side: string) => {
  const s = String(side || '').toUpperCase();
  if (s === 'BUY' || s === 'LONG') return { label: 'LONG', cls: 'bg-emerald-900 text-emerald-300 border border-emerald-700' };
  if (s === 'SELL' || s === 'SHORT') return { label: 'SHORT', cls: 'bg-rose-900 text-rose-300 border border-rose-700' };
  return { label: text(side), cls: 'bg-zinc-800 text-zinc-300 border border-zinc-700' };
};

const resultFromPnl = (pnlValue: number) => {
  if (pnlValue > 0) return { label: 'WIN', cls: 'bg-emerald-900 text-emerald-300 border border-emerald-700' };
  if (pnlValue < 0) return { label: 'LOSS', cls: 'bg-rose-900 text-rose-300 border border-rose-700' };
  return { label: 'BE', cls: 'bg-zinc-800 text-zinc-300 border border-zinc-700' };
};

export default function TradeTable({ items }: { items: any[] }) {
  const [symbol, setSymbol] = useState('ALL');
  const [side, setSide] = useState('ALL');
  const [status, setStatus] = useState('ALL');
  const [result, setResult] = useState('ALL');
  const [pnlView, setPnlView] = useState<'NET' | 'GROSS'>('NET');
  const [selected, setSelected] = useState<any>(null);

  const symbols = useMemo(() => Array.from(new Set(items.map((t) => String(t?.symbol || '')).filter(Boolean))).sort(), [items]);
  const hasNet = useMemo(() => items.some((t) => t?.pnl_net !== null && t?.pnl_net !== undefined), [items]);
  const hasGross = useMemo(() => items.some((t) => t?.pnl_gross !== null && t?.pnl_gross !== undefined), [items]);
  const canTogglePnl = hasNet && hasGross;

  const filtered = useMemo(() => {
    return items.filter((item) => {
      const rowSide = String(item?.side || '').toUpperCase();
      const rowStatus = String(item?.status || '').toUpperCase();
      const rowSymbol = String(item?.symbol || '');
      const pnlNet = Number(item?.pnl_net ?? item?.pnl ?? 0);
      const rowResult = pnlNet > 0 ? 'WIN' : pnlNet < 0 ? 'LOSS' : 'BE';
      return (
        (symbol === 'ALL' || rowSymbol === symbol) &&
        (side === 'ALL' || rowSide === side) &&
        (status === 'ALL' || rowStatus === status) &&
        (result === 'ALL' || rowResult === result)
      );
    });
  }, [items, symbol, side, status, result]);

  if (!items.length) return <div className='text-zinc-400'>No trades returned by API. count: 0</div>;

  return (
    <div className='space-y-3'>
      <div className='flex flex-wrap gap-2 text-xs'>
        <select value={symbol} onChange={(e) => setSymbol(e.target.value)} className='bg-zinc-950 border border-zinc-700 rounded px-2 py-1'>
          <option value='ALL'>All symbols</option>
          {symbols.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
        <select value={side} onChange={(e) => setSide(e.target.value)} className='bg-zinc-950 border border-zinc-700 rounded px-2 py-1'>
          <option value='ALL'>All sides</option>
          <option value='BUY'>BUY</option>
          <option value='SELL'>SELL</option>
        </select>
        <select value={status} onChange={(e) => setStatus(e.target.value)} className='bg-zinc-950 border border-zinc-700 rounded px-2 py-1'>
          <option value='ALL'>All statuses</option>
          <option value='OPEN'>OPEN</option>
          <option value='CLOSED'>CLOSED</option>
        </select>
        <select value={result} onChange={(e) => setResult(e.target.value)} className='bg-zinc-950 border border-zinc-700 rounded px-2 py-1'>
          <option value='ALL'>All results</option>
          <option value='WIN'>WIN</option>
          <option value='LOSS'>LOSS</option>
          <option value='BE'>BE</option>
        </select>
        {canTogglePnl && (
          <div className='flex items-center gap-1 border border-zinc-700 rounded px-2 py-1'>
            <span className='text-zinc-400'>PnL View</span>
            <button className={`px-2 py-0.5 rounded ${pnlView === 'NET' ? 'bg-zinc-700' : 'bg-zinc-900'}`} onClick={() => setPnlView('NET')}>Net</button>
            <button className={`px-2 py-0.5 rounded ${pnlView === 'GROSS' ? 'bg-zinc-700' : 'bg-zinc-900'}`} onClick={() => setPnlView('GROSS')}>Gross</button>
          </div>
        )}
      </div>

      <div className='max-w-full overflow-x-auto border border-zinc-800 rounded-lg'>
        <table className='w-full min-w-[2200px] text-xs'>
          <thead className='bg-zinc-900/95 sticky top-0 z-10'>
            <tr className='text-left border-b border-zinc-800'>
              <th className='p-2'>ID</th><th className='p-2'>Symbol</th><th className='p-2'>Side</th><th className='p-2'>Entry</th><th className='p-2'>Exit</th><th className='p-2'>SL</th><th className='p-2'>TP1</th><th className='p-2'>TP2</th><th className='p-2'>Qty</th><th className='p-2'>Notional</th><th className='p-2'>Fees</th><th className='p-2'>PnL (Net)</th><th className='p-2'>Result</th><th className='p-2'>Reason</th><th className='p-2'>Opened</th><th className='p-2'>Closed</th><th className='p-2'>Score</th><th className='p-2'>Regime</th>
            </tr>
          </thead>
          <tbody className='font-mono'>
            {filtered.map((item) => {
              const side = sideInfo(item?.side);
              const pnlNet = Number(item?.pnl_net ?? item?.pnl ?? 0);
              const pnlGross = Number(item?.pnl_gross ?? item?.pnl ?? 0);
              const pnlShown = pnlView === 'NET' ? pnlNet : pnlGross;
              const result = resultFromPnl(pnlNet);
              const notional = item?.notional ?? (Number(item?.quantity || 0) * Number(item?.entry_price || 0));
              return (
                <tr key={item?.id} className='border-b border-zinc-900 hover:bg-zinc-900/50 cursor-pointer' onClick={() => setSelected(item)}>
                  <td className='p-2'>{text(item?.id).slice(0, 8)}</td>
                  <td className='p-2'>{text(item?.symbol)}</td>
                  <td className='p-2'><span className={`px-2 py-0.5 rounded text-[11px] ${side.cls}`}>{side.label}</span></td>
                  <td className='p-2'>{fmtNum(item?.entry_price, 2)}</td>
                  <td className='p-2'>{fmtNum(item?.exit_price, 2)}</td>
                  <td className='p-2'>{fmtNum(item?.stop_price, 2)}</td>
                  <td className='p-2'>{fmtNum(item?.tp1_price, 2)}</td>
                  <td className='p-2'>{fmtNum(item?.tp2_price, 2)}</td>
                  <td className='p-2'>{fmtNum(item?.quantity, 4)}</td>
                  <td className='p-2'>{fmtNum(notional, 2)}</td>
                  <td className='p-2'>{fmtNum(item?.fees_total ?? item?.fees, 2)}</td>
                  <td className='p-2'>{fmtNum(pnlShown, 2)}</td>
                  <td className='p-2'><span className={`px-2 py-0.5 rounded text-[11px] ${result.cls}`}>{result.label}</span></td>
                  <td className='p-2'>{text(item?.close_reason)}</td>
                  <td className='p-2'>{fmtTs(item?.opened_at, item?.created_at)}</td>
                  <td className='p-2'>{fmtTs(item?.closed_at, item?.created_at)}</td>
                  <td className='p-2'>{fmtNum(item?.score_at_entry, 2)}</td>
                  <td className='p-2'>{text(item?.regime_at_entry)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {selected && (
        <div className='bg-zinc-950 border border-zinc-800 rounded-lg p-3 text-xs'>
          <div className='flex items-center justify-between mb-2'>
            <strong>Trade Detail</strong>
            <button className='px-2 py-1 border border-zinc-700 rounded' onClick={() => setSelected(null)}>Close</button>
          </div>
          <pre className='max-w-full overflow-x-auto whitespace-pre-wrap break-words'>{JSON.stringify(selected, null, 2)}</pre>
        </div>
      )}
    </div>
  );
}
