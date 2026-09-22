export type {
  MarketDataPoint,
  TechnicalIndicator,
  MarketOverview,
  SectorPerformance,
  TopMover,
  TimeSeriesPoint,
  ChartDataPoint,
} from "./market-data"

export type {
  PortfolioPosition,
  PortfolioSummary,
  AllocationBreakdown,
  PerformanceMetrics,
  RecentActivity,
} from "./portfolio"

export type {
  AlertType,
  AlertSeverity,
  AlertStatus,
  Alert,
  AlertRule,
  AlertStatistics,
} from "./alerts"

export type {
  SystemMetrics,
  ServiceStatus,
  SystemHealth,
} from "./system"

export interface KpiCardData {
  readonly title: string
  readonly value: string
  readonly change: number
  readonly changeLabel: string
  readonly icon: string
  readonly trend: readonly number[]
}
