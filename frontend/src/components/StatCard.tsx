'use client'
import { useEffect, useRef, memo } from 'react'
import gsap from 'gsap'

interface StatCardProps {
  label: string
  value: number | undefined
  format: (n: number) => string
  sub?: string
  subUp?: boolean | null
  icon: React.ReactNode
}

function StatCard({ label, value, format, sub, subUp, icon }: StatCardProps) {
  const valRef = useRef<HTMLSpanElement>(null)
  const cardRef = useRef<HTMLDivElement>(null)
  const tweenObj = useRef({ n: value ?? 0 })
  const prevRef = useRef<number | undefined>(value)

  useEffect(() => {
    if (value === undefined || !valRef.current) return

    const from = prevRef.current ?? value
    const changePct = from > 0 ? Math.abs((value - from) / from) * 100 : 0

    // Counter tween — overwrite kills any in-progress tween first
    tweenObj.current.n = from
    gsap.to(tweenObj.current, {
      n: value,
      duration: 0.55,
      ease: 'power2.out',
      overwrite: true,
      onUpdate() {
        if (valRef.current) valRef.current.textContent = format(tweenObj.current.n)
      },
    })

    // Border flash only on meaningful price changes (avoids rapid micro-flickers)
    if (prevRef.current !== undefined && cardRef.current && changePct > 0.005) {
      const flashColor = value > prevRef.current ? '#22c55e33' : '#ef444433'
      gsap.fromTo(
        cardRef.current,
        { boxShadow: `inset 0 0 0 1px ${flashColor}` },
        { boxShadow: 'inset 0 0 0 1px transparent', duration: 1, ease: 'power2.out', overwrite: true },
      )
    }

    prevRef.current = value
  }, [value, format])

  const subColor =
    subUp === true ? 'text-crypto-green' :
    subUp === false ? 'text-crypto-red' :
    'text-crypto-dim'

  return (
    <div
      ref={cardRef}
      className="bg-crypto-card border border-crypto-border rounded-xl p-4 flex flex-col gap-1"
      style={{ borderWidth: '0.5px' }}
    >
      <div className="flex items-center gap-1.5 text-[11px] text-crypto-dim">
        {icon}
        {label}
      </div>
      <div className="text-2xl font-semibold text-crypto-text leading-tight tabular-nums">
        <span ref={valRef}>{value !== undefined ? format(value) : '—'}</span>
      </div>
      {sub && <div className={`text-xs ${subColor}`}>{sub}</div>}
    </div>
  )
}

// Memo: skip re-render if value and sub didn't meaningfully change
export default memo(StatCard, (prev, next) =>
  prev.value === next.value && prev.sub === next.sub && prev.subUp === next.subUp
)
