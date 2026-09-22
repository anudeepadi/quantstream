export interface PortfolioPosition {
  readonly symbol: string
  readonly name: string
  readonly quantity: number
  readonly avg_cost: number
  readonly current_price: number
  readonly market_value: number
  readonly unrealized_pnl: number
  readonly unrealized_pnl_percent: number
  readonly weight: number
  readonly sector: string
}

export interface PortfolioSummary {
  readonly total_value: number
  readonly total_cost: number
  readonly total_pnl: number
  readonly total_pnl_percent: number
  readonly daily_pnl: number
  readonly daily_pnl_percent: number
  readonly cash: number
  readonly buying_power: number
}

export interface AllocationBreakdown {
  readonly category: string
  readonly value: number
  readonly percentage: number
  readonly color: string
}

export interface PerformanceMetrics {
  readonly period: string
  readonly return_percent: number
  readonly benchmark_return_percent: number
  readonly alpha: number
  readonly sharpe_ratio: number
  readonly max_drawdown: number
  readonly volatility: number
}

export interface RecentActivity {
  readonly id: string
  readonly type: "buy" | "sell" | "dividend" | "deposit" | "withdrawal"
  readonly symbol: string
  readonly description: string
  readonly amount: number
  readonly timestamp: string
}
