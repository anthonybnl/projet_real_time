import dynamic from 'next/dynamic'

// Dashboard uses WebSocket + GSAP — must be client-only (no SSR)
const Dashboard = dynamic(() => import('@/components/Dashboard'), { ssr: false })

export default function Home() {
  return <Dashboard />
}
