'use client';

import { useEffect, useState } from 'react';

import ModeBadge from '../components/ModeBadge';
import TradeTable from '../components/TradeTable';
import { fetchEngineStatus, fetchOverview, fetchTrades } from '../lib/api';

export default function HomePage() {
  const [overview, setOverview] = useState<any>(null);
  const [trades, setTrades] = useState<any[]>([]);
  const [status, setStatus] = useState<any>(null);

  useEffect(() => {
    const load = async () => {
      const [ov, tr, st] = await Promise.all([fetchOverview(), fetchTrades(50, 0), fetchEngineStatus()]);
      setOverview(ov);
      setTrades(tr.items || []);
      setStatus(st);
    };

    load();
    const timer = setInterval(load, 3000);
    return () => clearInterval(timer);
  }, []);

  return (
    <main className='p-6 space-y-4'>
      <h1 className='text-2xl font-bold'>Hashi Trading Dashboard</h1>
      <div className='grid grid-cols-2 md:grid-cols-4 gap-3'>
        <div className='p-3 bg-gray-900 rounded'>Equity: {overview?.equity ?? '-'}</div>
        <div className='p-3 bg-gray-900 rounded'>Daily DD %: {overview?.daily_dd_pct ?? '-'}</div>
        <div className='p-3 bg-gray-900 rounded'>Global DD %: {overview?.global_dd_pct ?? '-'}</div>
        <div className='p-3 bg-gray-900 rounded'>Monthly Progress %: {overview?.monthly_progress_pct ?? '-'}</div>
        <div className='p-3 bg-gray-900 rounded'>Open Positions: {overview?.open_positions ?? '-'}</div>
        <div className='p-3 bg-gray-900 rounded'>Leverage: {overview?.leverage ?? '-'}</div>
        <div className='p-3 bg-gray-900 rounded'>Risk State: {overview?.risk_state?.can_trade ? 'ACTIVE' : 'BLOCKED'}</div>
        <div className='p-3 bg-gray-900 rounded'>Mode: <ModeBadge mode={overview?.mode || 'LIVE'} /></div>
      </div>
      <div className='p-4 bg-gray-900 rounded'>
        <h2 className='font-semibold mb-2'>Trade Log</h2>
        <TradeTable items={trades} />
      </div>
      <pre className='p-3 bg-black rounded text-xs overflow-x-auto'>{JSON.stringify(status, null, 2)}</pre>
    </main>
  );
}
