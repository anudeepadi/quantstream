import type { Alert, AlertRule, AlertStatistics } from "@/lib/types/alerts"

export const MOCK_ALERT_STATISTICS: AlertStatistics = {
  total_alerts: 47,
  active_alerts: 12,
  triggered_today: 4,
  critical_count: 2,
  alerts_by_type: { price: 18, volume: 10, technical: 8, news: 6, system: 5 },
  alerts_by_severity: { info: 15, warning: 20, critical: 12 },
  avg_resolution_time_minutes: 45,
}

export const MOCK_ALERTS: readonly Alert[] = [
  { id: "1", type: "price", severity: "critical", status: "acknowledged", title: "TSLA Price Drop", message: "TSLA has dropped below $200 support level", symbol: "TSLA", created_at: "2026-02-19T10:00:00Z", triggered_at: "2026-02-19T14:22:00Z" },
  { id: "2", type: "volume", severity: "warning", status: "active", title: "NVDA Volume Spike", message: "NVDA trading volume 3x above average", symbol: "NVDA", created_at: "2026-02-19T09:30:00Z", triggered_at: null },
  { id: "3", type: "technical", severity: "info", status: "active", title: "AAPL RSI Overbought", message: "AAPL RSI has crossed above 70", symbol: "AAPL", created_at: "2026-02-18T15:00:00Z", triggered_at: null },
  { id: "4", type: "price", severity: "critical", status: "acknowledged", title: "Portfolio Drawdown", message: "Portfolio drawdown exceeds 5% threshold", symbol: null, created_at: "2026-02-18T11:00:00Z", triggered_at: "2026-02-18T14:30:00Z" },
  { id: "5", type: "news", severity: "info", status: "active", title: "GOOGL Earnings Report", message: "Alphabet earnings report due in 2 days", symbol: "GOOGL", created_at: "2026-02-17T08:00:00Z", triggered_at: null },
  { id: "6", type: "system", severity: "warning", status: "dismissed", title: "API Latency High", message: "Market data API response time exceeding 500ms", symbol: null, created_at: "2026-02-16T12:00:00Z", triggered_at: "2026-02-16T12:15:00Z" },
]

export const MOCK_ALERT_RULES: readonly AlertRule[] = [
  { id: "r1", name: "Price Below Support", type: "price", condition: "price < threshold", threshold: 200, symbol: "TSLA", enabled: true, created_at: "2026-01-15T10:00:00Z" },
  { id: "r2", name: "Volume 3x Average", type: "volume", condition: "volume > 3 * avg_volume", threshold: 3, symbol: null, enabled: true, created_at: "2026-01-10T09:00:00Z" },
  { id: "r3", name: "RSI Overbought", type: "technical", condition: "rsi > threshold", threshold: 70, symbol: null, enabled: true, created_at: "2026-01-05T08:00:00Z" },
  { id: "r4", name: "Portfolio Drawdown", type: "price", condition: "drawdown > threshold%", threshold: 5, symbol: null, enabled: true, created_at: "2025-12-20T10:00:00Z" },
]
