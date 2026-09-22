import { apiGet, isMockMode } from "./client"
import type { SystemHealth } from "@/lib/types/system"

const MOCK_SYSTEM_HEALTH: SystemHealth = {
  overall_status: "healthy",
  services: [
    { name: "FastAPI Backend", status: "healthy", latency_ms: 12, last_check: "2026-02-19T14:30:00Z", message: null },
    { name: "PostgreSQL", status: "healthy", latency_ms: 3, last_check: "2026-02-19T14:30:00Z", message: null },
    { name: "Redis Cache", status: "healthy", latency_ms: 1, last_check: "2026-02-19T14:30:00Z", message: null },
    { name: "Kafka Broker", status: "degraded", latency_ms: 85, last_check: "2026-02-19T14:30:00Z", message: "High consumer lag detected" },
    { name: "Spark Cluster", status: "healthy", latency_ms: 45, last_check: "2026-02-19T14:30:00Z", message: null },
    { name: "Market Data Feed", status: "healthy", latency_ms: 28, last_check: "2026-02-19T14:30:00Z", message: null },
  ],
  metrics: {
    cpu_usage: 42.5,
    memory_usage: 68.2,
    disk_usage: 55.8,
    network_in: 125.4,
    network_out: 89.2,
    uptime_seconds: 1728000,
    active_connections: 156,
    requests_per_second: 342,
  },
  last_updated: "2026-02-19T14:30:00Z",
}

export async function fetchSystemHealth(): Promise<SystemHealth> {
  if (isMockMode()) return MOCK_SYSTEM_HEALTH
  return apiGet<SystemHealth>("/api/v1/system/health")
}
