export type AlertType = "price" | "volume" | "technical" | "news" | "system"
export type AlertSeverity = "info" | "warning" | "critical"
export type AlertStatus = "active" | "acknowledged" | "resolved" | "dismissed"

export interface Alert {
  readonly id: string
  readonly type: AlertType
  readonly severity: AlertSeverity
  readonly status: AlertStatus
  readonly title: string
  readonly message: string
  readonly symbol: string | null
  readonly created_at: string
  readonly triggered_at: string | null
}

export interface AlertRule {
  readonly id: string
  readonly name: string
  readonly type: AlertType
  readonly condition: string
  readonly threshold: number
  readonly symbol: string | null
  readonly enabled: boolean
  readonly created_at: string
}

export interface AlertStatistics {
  readonly total_alerts: number
  readonly active_alerts: number
  readonly triggered_today: number
  readonly critical_count: number
  readonly alerts_by_type: Record<string, number>
  readonly alerts_by_severity: Record<string, number>
  readonly avg_resolution_time_minutes: number
}
