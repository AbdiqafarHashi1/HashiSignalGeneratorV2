'use client';

export default function TradeTable({ items }: { items: any[] }) {
  return (
    <table className='w-full text-sm'>
      <thead>
        <tr className='text-left border-b border-gray-700'>
          <th>Symbol</th><th>Side</th><th>Qty</th><th>Status</th><th>Created</th>
        </tr>
      </thead>
      <tbody>
        {items.map((item) => (
          <tr key={item.id} className='border-b border-gray-800'>
            <td>{item.symbol}</td><td>{item.side}</td><td>{item.quantity}</td><td>{item.status}</td><td>{item.created_at}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
