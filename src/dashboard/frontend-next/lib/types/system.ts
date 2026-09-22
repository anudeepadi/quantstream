export interface SystemMetrics {
  readonly cpu_usage: number
  readonly memory_usage: number
  readonly disk_usage: number
  readonly network_in: number
  readonly network_out: number
  readonly uptime_seconds: number
  readonly active_connections: number
  readonly requests_per_second: number
}

export interface ServiceStatus {
  readonly name: string
  readonly status: "healthy" | "degraded" | "down"
  readonly latency_ms: number
  readonly last_check: string
  readonly message: string | null
}

export interface SystemHealth {
  readonly overall_status: "healthy" | "degraded" | "down"
  readonly services: readonly ServiceStatus[]
  readonly metrics: SystemMetrics
  readonly last_updated: string
}
