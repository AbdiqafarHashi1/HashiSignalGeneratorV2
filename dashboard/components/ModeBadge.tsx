export default function ModeBadge({ mode }: { mode: string }) {
  const tone = mode === 'LIVE' ? 'bg-green-600' : 'bg-blue-600';
  return <span className={`px-2 py-1 rounded text-white text-xs ${tone}`}>{mode}</span>;
}
