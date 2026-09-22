import { apiGet, isMockMode } from "./client"
import type { Alert, AlertRule, AlertStatistics } from "@/lib/types/alerts"
import { MOCK_ALERTS, MOCK_ALERT_RULES, MOCK_ALERT_STATISTICS } from "@/lib/mock-data/alerts"

export async function fetchAlerts(): Promise<readonly Alert[]> {
  if (isMockMode()) return MOCK_ALERTS
  return apiGet<Alert[]>("/api/v1/alerts/")
}

export async function fetchAlertRules(): Promise<readonly AlertRule[]> {
  if (isMockMode()) return MOCK_ALERT_RULES
  return apiGet<AlertRule[]>("/api/v1/alerts/rules/")
}

export async function fetchAlertStatistics(): Promise<AlertStatistics> {
  if (isMockMode()) return MOCK_ALERT_STATISTICS
  return apiGet<AlertStatistics>("/api/v1/alerts/statistics/")
}
