'use client'
import { useEffect, useRef } from 'react'
import gsap from 'gsap'
import type { Trade } from '@/types'

function fmtTime(ts: number) {
  return new Date(ts * 1000).toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

interface TradeRowProps { trade: Trade }

function TradeRow({ trade }: TradeRowProps) {
  const rowRef = useRef<HTMLTableRowElement>(null)

  useEffect(() => {
    if (!rowRef.current) return
    const flashColor = trade.side === 'buy' ? 'rgba(34,197,94,0.12)' : 'rgba(239,68,68,0.12)'
    gsap.fromTo(
      rowRef.current,
      { backgroundColor: flashColor, y: -8, opacity: 0 },
      { backgroundColor: 'transparent', y: 0, opacity: 1, duration: 0.4, ease: 'power2.out' },
    )
  }, [trade.side])

  return (
    <tr ref={rowRef}>
      <td className="py-1.5 pr-2">
        <span
          className="inline-block px-2 py-0.5 rounded text-[11px] font-medium"
          style={{
            background: trade.side === 'buy' ? 'rgba(34,197,94,0.15)' : 'rgba(239,68,68,0.15)',
            color: trade.side === 'buy' ? '#22c55e' : '#ef4444',
          }}
        >
          {trade.side}
        </span>
      </td>
      <td className="py-1.5 font-mono text-xs text-crypto-text">
        ${trade.price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
      </td>
      <td className="py-1.5 font-mono text-xs text-crypto-text text-right">
        {trade.volume.toFixed(4)}
      </td>
      <td className="py-1.5 text-[11px] text-crypto-dim text-right font-mono">
        {fmtTime(trade.timestamp)}
      </td>
    </tr>
  )
}

interface TradesTableProps { trades: Trade[] }

function TradesTable({ trades }: TradesTableProps) {
  return (
    <div className="bg-crypto-card border border-crypto-border rounded-xl p-4 flex flex-col" style={{ borderWidth: '0.5px' }}>
      <div className="flex justify-between items-center mb-3">
        <span className="text-xs font-medium text-crypto-text">Recent trades</span>
        <span className="text-[11px] text-crypto-dim">Last {trades.length}</span>
      </div>

      <div className="overflow-y-auto max-h-48">
        <table className="w-full border-collapse text-xs table-fixed">
          <thead>
            <tr className="border-b border-crypto-border">
              <th className="pb-2 text-left text-crypto-dim font-normal w-14">Side</th>
              <th className="pb-2 text-left text-crypto-dim font-normal">Price</th>
              <th className="pb-2 text-right text-crypto-dim font-normal">Qty</th>
              <th className="pb-2 text-right text-crypto-dim font-normal w-16">Time</th>
            </tr>
          </thead>
          <tbody>
            {trades.length === 0 ? (
              <tr>
                <td colSpan={4} className="py-4 text-center text-crypto-dim text-[11px]">
                  Waiting for trades…
                </td>
              </tr>
            ) : (
              trades.map((t) => (
                <TradeRow key={`${t.symbol}-${t.timestamp}-${t.price}`} trade={t} />
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default TradesTable
