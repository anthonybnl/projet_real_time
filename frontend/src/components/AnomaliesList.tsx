'use client'
import { useEffect, useRef, useState, useCallback } from 'react'
import { createPortal } from 'react-dom'
import gsap from 'gsap'
import type { AnomalyData, AnomalyTrigger, AnomalyType } from '@/types'

function fmt(ts: number) {
  return new Date(ts * 1000).toLocaleTimeString('fr-FR', {
    hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit',
  })
}

function fmtDate(ts: number) {
  return new Date(ts * 1000).toLocaleString('fr-FR', {
    day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', second: '2-digit',
  })
}

const TYPE_LABELS: Record<AnomalyType, string> = {
  SLIPPAGE_ANORMAL: 'Glissement anormal',
  SPREAD_ELASTIQUE: 'Spread élastique',
  ORDER_FLOW_IMBALANCE: 'Déséquilibre de flux',
  COMBINED_ANOMALY: 'Anomalie combinée',
  DEPTH_EROSION: 'Érosion de profondeur',
  VPIN: 'VPIN',
  WHALE_ALERT: 'Ordre massif',
}

const BADGE_LABELS: Record<AnomalyType, string> = {
  SLIPPAGE_ANORMAL: 'GLISSEMENT',
  SPREAD_ELASTIQUE: 'SPREAD',
  ORDER_FLOW_IMBALANCE: 'FLUX',
  COMBINED_ANOMALY: 'COMBINÉE',
  DEPTH_EROSION: 'PROFONDEUR',
  VPIN: 'VPIN',
  WHALE_ALERT: 'BALEINE',
}

const DETAIL_LABELS: Record<string, string> = {
  price: 'Prix',
  previous_price: 'Prix précédent',
  log_return: 'Rendement log.',
  z_score: 'Z-score',
  adaptive_threshold_hi: 'Seuil haut',
  adaptive_threshold_lo: 'Seuil bas',
  intensity: 'Intensité',
  bid: 'Offre (bid)',
  ask: 'Demande (ask)',
  current_spread: 'Spread actuel',
  adaptive_threshold: 'Seuil adaptatif',
  rolling_mean_spread: 'Spread moyen',
  ratio: 'Ratio',
  vol_buy: 'Volume acheteurs',
  vol_sell: 'Volume vendeurs',
  vol_total: 'Volume total',
  ofi_ratio: 'Ratio OFI',
  threshold: 'Seuil',
  confidence: 'Confiance',
  active_rules: 'Règles actives',
  n_rules: 'Nombre de règles',
  combo_window_sec: 'Fenêtre (s)',
  sensitivity: 'Sensibilité',
  volume: 'Volume',
  weighted_score: 'Score pondéré',
  vpin: 'VPIN',
  depth_ratio: 'Ratio profondeur',
}

const STYLES: Record<AnomalyType, { accent: string; bg: string; badgeBg: string; badgeText: string }> = {
  SLIPPAGE_ANORMAL: {
    accent: '#f59e0b',
    bg: 'rgba(245,158,11,0.07)',
    badgeBg: 'rgba(245,158,11,0.18)',
    badgeText: '#fcd34d',
  },
  SPREAD_ELASTIQUE: {
    accent: '#a855f7',
    bg: 'rgba(168,85,247,0.07)',
    badgeBg: 'rgba(168,85,247,0.18)',
    badgeText: '#d8b4fe',
  },
  ORDER_FLOW_IMBALANCE: {
    accent: '#3b82f6',
    bg: 'rgba(59,130,246,0.07)',
    badgeBg: 'rgba(59,130,246,0.18)',
    badgeText: '#93c5fd',
  },
  COMBINED_ANOMALY: {
    accent: '#ef4444',
    bg: 'rgba(239,68,68,0.07)',
    badgeBg: 'rgba(239,68,68,0.18)',
    badgeText: '#fca5a5',
  },
  DEPTH_EROSION: {
    accent: '#f97316',
    bg: 'rgba(249,115,22,0.07)',
    badgeBg: 'rgba(249,115,22,0.18)',
    badgeText: '#fdba74',
  },
  VPIN: {
    accent: '#eab308',
    bg: 'rgba(234,179,8,0.07)',
    badgeBg: 'rgba(234,179,8,0.18)',
    badgeText: '#fde047',
  },
  WHALE_ALERT: {
    accent: '#ef4444',
    bg: 'rgba(239,68,68,0.07)',
    badgeBg: 'rgba(239,68,68,0.18)',
    badgeText: '#fca5a5',
  },
}

const DEFAULT_STYLE = STYLES.ORDER_FLOW_IMBALANCE

function anomalyStyle(type: AnomalyType) {
  return STYLES[type] ?? DEFAULT_STYLE
}

function anomalyKey(a: AnomalyData) {
  return a.id ?? `${a.anomaly_type}-${a.timestamp}`
}

function anomalyBadgeLabel(a: AnomalyData): string {
  return BADGE_LABELS[a.anomaly_type] ?? a.anomaly_type
}

function typeLabel(a: AnomalyData): string {
  return TYPE_LABELS[a.anomaly_type] ?? a.anomaly_type
}

function anomalyMessage(a: AnomalyData): string {
  const desc = a.details?.description
  if (typeof desc === 'string' && desc.length > 0) return desc
  return `${typeLabel(a)} · ${a.symbol}`
}

function formatDetailValue(key: string, value: unknown): string {
  if (value === null || value === undefined) return '—'
  if (Array.isArray(value)) return value.join(', ')
  if (typeof value === 'object') return JSON.stringify(value)
  if (typeof value === 'number') {
    if (key === 'confidence') return `${Math.round(value)}/100`
    if (key.includes('ratio') || key.includes('intensity') || key === 'z_score') {
      return value.toFixed(2)
    }
    if (Math.abs(value) < 0.01 || value > 1000) return value.toExponential(2)
    return value.toLocaleString('fr-FR', { maximumFractionDigits: 4 })
  }
  return String(value)
}

const SIDE_LABELS: Record<string, { label: string; color: string }> = {
  buy: { label: 'Achat', color: '#34d399' },
  sell: { label: 'Vente', color: '#f87171' },
}

function triggerSide(t: AnomalyTrigger): { label: string; color: string } | null {
  let side = typeof t.side === 'string' ? t.side.toLowerCase() : null
  if (!side && typeof t.m === 'boolean') side = t.m ? 'sell' : 'buy'
  if (!side) return null
  return SIDE_LABELS[side] ?? { label: side, color: '#cbd5e1' }
}

function num(value: unknown): number | null {
  const n = typeof value === 'string' ? Number(value) : value
  return typeof n === 'number' && Number.isFinite(n) ? n : null
}

function triggerFields(t: AnomalyTrigger): { key: string; label: string; value: string }[] {
  const fields: { key: string; label: string; value: string }[] = []

  const price = num(t.price ?? t.p)
  if (price !== null) {
    fields.push({ key: 'price', label: 'Prix', value: price.toLocaleString('fr-FR', { maximumFractionDigits: 2 }) })
  }

  const size = num(t.trade_size ?? t.volume ?? t.size ?? t.q)
  if (size !== null) {
    fields.push({ key: 'trade_size', label: 'Taille du trade', value: size.toLocaleString('fr-FR', { maximumFractionDigits: 8 }) })
  }

  return fields
}

function popoverFields(anomaly: AnomalyData): { key: string; label: string; value: string }[] {
  const skip = new Set(['description', 'rule_intensities', 'rule_weights'])
  return Object.entries(anomaly.details ?? {})
    .filter(([k]) => !skip.has(k))
    .map(([key, value]) => ({
      key,
      label: DETAIL_LABELS[key] ?? key.replace(/_/g, ' '),
      value: formatDetailValue(key, value),
    }))
}

function AnomalyPopover({
  anomaly,
  onClose,
}: {
  anomaly: AnomalyData
  onClose: () => void
}) {
  const s = anomalyStyle(anomaly.anomaly_type)
  const fields = popoverFields(anomaly)
  const trigger = anomaly.trigger_message ?? null
  const triggerData = trigger ? triggerFields(trigger) : []
  const side = trigger ? triggerSide(trigger) : null

  return (
    <div
      className="relative w-[min(420px,88vmin)] aspect-square flex flex-col rounded-2xl overflow-hidden
        border border-white/[0.12] bg-[rgba(13,21,38,0.55)] backdrop-blur-2xl
        shadow-[0_24px_80px_rgba(0,0,0,0.55),inset_0_1px_0_rgba(255,255,255,0.06)]"
      role="dialog"
      aria-label="Détails de l'anomalie"
      onClick={(e) => e.stopPropagation()}
    >
      <div
        className="absolute inset-x-0 top-0 h-24 pointer-events-none opacity-40"
        style={{ background: `radial-gradient(ellipse at top, ${s.accent}33, transparent 70%)` }}
      />

      <div
        className="relative shrink-0 flex items-start justify-between gap-3 px-5 py-4 border-b border-white/[0.08]"
        style={{ borderLeft: `3px solid ${s.accent}` }}
      >
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span
              className="text-[10px] font-bold px-2 py-0.5 rounded-md backdrop-blur-sm"
              style={{ background: s.badgeBg, color: s.badgeText }}
            >
              {anomalyBadgeLabel(anomaly)}
            </span>
            <p className="text-sm font-semibold text-crypto-text leading-tight">
              {typeLabel(anomaly)}
            </p>
          </div>
          <p className="text-[11px] text-crypto-dim mt-1.5">
            {anomaly.exchange} · {anomaly.symbol}
          </p>
          <p className="text-[10px] text-crypto-dim/80 font-mono mt-0.5">
            {fmtDate(anomaly.timestamp)}
          </p>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="shrink-0 w-8 h-8 flex items-center justify-center rounded-full
            text-crypto-dim hover:text-crypto-text
            bg-white/[0.06] hover:bg-white/[0.1] border border-white/[0.08]
            text-lg leading-none transition-colors"
          aria-label="Fermer"
        >
          ×
        </button>
      </div>

      <div className="relative flex-1 min-h-0 overflow-y-auto px-5 py-4 flex flex-col gap-4">
        {typeof anomaly.details?.description === 'string' && (
          <p className="text-[13px] text-crypto-text leading-relaxed">
            {anomaly.details.description}
          </p>
        )}

        {fields.length > 0 && (
          <dl className="grid grid-cols-2 gap-x-4 gap-y-3">
            {fields.map(({ key, label, value }) => (
              <div
                key={key}
                className="min-w-0 rounded-lg px-2.5 py-2 bg-white/[0.04] border border-white/[0.06]"
              >
                <dt className="text-[9px] uppercase tracking-wide text-crypto-dim">{label}</dt>
                <dd className="text-[12px] text-crypto-text font-mono tabular-nums mt-1 break-all leading-snug">
                  {value}
                </dd>
              </div>
            ))}
          </dl>
        )}

        {(triggerData.length > 0 || side) && (
          <div className="flex flex-col gap-2">
            <div className="flex items-center gap-2">
              <span className="text-[9px] uppercase tracking-wide text-crypto-dim">Trade déclencheur</span>
              <span className="flex-1 h-px bg-white/[0.08]" />
              {side && (
                <span
                  className="text-[9px] font-bold px-1.5 py-0.5 rounded uppercase tracking-wide"
                  style={{ background: `${side.color}22`, color: side.color }}
                >
                  {side.label}
                </span>
              )}
            </div>
            <dl className="grid grid-cols-2 gap-x-4 gap-y-3">
              {triggerData.map(({ key, label, value }) => (
                <div
                  key={key}
                  className="min-w-0 rounded-lg px-2.5 py-2 bg-white/[0.04] border border-white/[0.06]"
                >
                  <dt className="text-[9px] uppercase tracking-wide text-crypto-dim">{label}</dt>
                  <dd className="text-[12px] text-crypto-text font-mono tabular-nums mt-1 break-all leading-snug">
                    {value}
                  </dd>
                </div>
              ))}
            </dl>
          </div>
        )}
      </div>
    </div>
  )
}

function AnomalyPopoverOverlay({
  anomaly,
  onClose,
}: {
  anomaly: AnomalyData
  onClose: () => void
}) {
  const [mounted, setMounted] = useState(false)
  const cardRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    setMounted(true)
    const prev = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      document.body.style.overflow = prev
    }
  }, [])

  useEffect(() => {
    if (!mounted || !cardRef.current) return
    gsap.fromTo(
      cardRef.current,
      { scale: 0.92, opacity: 0 },
      { scale: 1, opacity: 1, duration: 0.28, ease: 'power2.out' },
    )
  }, [mounted])

  if (!mounted) return null

  return createPortal(
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-4">
      <button
        type="button"
        className="absolute inset-0 bg-black/45 backdrop-blur-md"
        aria-label="Fermer"
        onClick={onClose}
      />
      <div ref={cardRef} className="relative">
        <AnomalyPopover anomaly={anomaly} onClose={onClose} />
      </div>
    </div>,
    document.body,
  )
}

function AnomalyRow({
  anomaly,
  selected,
  onSelect,
}: {
  anomaly: AnomalyData
  selected: boolean
  onSelect: () => void
}) {
  const rowRef = useRef<HTMLButtonElement>(null)
  const s = anomalyStyle(anomaly.anomaly_type)

  useEffect(() => {
    if (!rowRef.current) return
    gsap.fromTo(
      rowRef.current,
      { x: -20, opacity: 0 },
      { x: 0, opacity: 1, duration: 0.35, ease: 'power2.out' },
    )
  }, [])

  return (
    <button
      type="button"
      ref={rowRef}
      onClick={onSelect}
      className="w-full text-left flex items-center gap-2.5 px-2.5 py-2 rounded-lg transition-opacity cursor-pointer hover:opacity-90"
      style={{
        background: s.bg,
        borderLeft: `2.5px solid ${s.accent}`,
        outline: selected ? `1px solid ${s.accent}40` : undefined,
      }}
    >
      <span
        className="shrink-0 text-[10px] font-bold px-1.5 py-0.5 rounded leading-tight tabular-nums"
        style={{ background: s.badgeBg, color: s.badgeText }}
      >
        {anomalyBadgeLabel(anomaly)}
      </span>

      <div className="flex-1 min-w-0">
        <span className="block text-[11px] text-crypto-text truncate leading-tight">
          {anomalyMessage(anomaly)}
        </span>
        <span className="block text-[10px] text-crypto-dim truncate leading-tight">
          {anomaly.exchange} · {anomaly.symbol}
        </span>
      </div>

      <span className="shrink-0 text-[10px] text-crypto-dim font-mono">
        {fmt(anomaly.timestamp)}
      </span>
    </button>
  )
}

interface AnomaliesListProps { anomalies: AnomalyData[] }

function AnomaliesList({ anomalies }: AnomaliesListProps) {
  const [selectedKey, setSelectedKey] = useState<string | null>(null)

  const selected = anomalies.find((a) => anomalyKey(a) === selectedKey) ?? null

  const closePopover = useCallback(() => setSelectedKey(null), [])

  useEffect(() => {
    if (!selectedKey) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closePopover()
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [selectedKey, closePopover])

  return (
    <>
      {selected && (
        <AnomalyPopoverOverlay anomaly={selected} onClose={closePopover} />
      )}

      <div
        className="bg-crypto-card border border-crypto-border rounded-xl p-4 flex flex-col"
        style={{ borderWidth: '0.5px' }}
      >
        <div className="flex justify-between items-center mb-3">
          <span className="text-xs font-medium text-crypto-text">Anomalies</span>
          <div className="flex items-center gap-2 text-[10px] text-crypto-dim">
            <span className="w-1.5 h-1.5 rounded-full bg-crypto-green animate-pulse inline-block" />
            En direct
          </div>
        </div>

        <div className="flex flex-col gap-1.5 overflow-y-auto max-h-48">
        {anomalies.length === 0 ? (
          <p className="text-[11px] text-crypto-dim text-center py-4">Aucune anomalie détectée</p>
        ) : (
          anomalies.map((a) => {
            const key = anomalyKey(a)
            return (
              <AnomalyRow
                key={key}
                anomaly={a}
                selected={selectedKey === key}
                onSelect={() => setSelectedKey(selectedKey === key ? null : key)}
              />
            )
          })
        )}
        </div>
      </div>
    </>
  )
}

export default AnomaliesList
