const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000"
const USE_MOCK = process.env.NEXT_PUBLIC_USE_MOCK_DATA === "true"

export function isMockMode(): boolean {
  return USE_MOCK
}

function getAuthHeaders(): Record<string, string> {
  const headers: Record<string, string> = { "Content-Type": "application/json" }

  if (typeof window !== "undefined") {
    const token = localStorage.getItem("qs_token")
    if (token) {
      headers["Authorization"] = `Bearer ${token}`
    }
  }

  return headers
}

export async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: getAuthHeaders(),
  })

  if (response.status === 401) {
    if (typeof window !== "undefined") {
      localStorage.removeItem("qs_token")
      localStorage.removeItem("qs_refresh")
      window.location.href = "/login"
    }
    throw new Error("Unauthorized")
  }

  if (!response.ok) {
    throw new Error(`API error ${response.status}: ${response.statusText}`)
  }

  return response.json() as Promise<T>
}

export async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })

  if (response.status === 401) {
    if (typeof window !== "undefined") {
      localStorage.removeItem("qs_token")
      localStorage.removeItem("qs_refresh")
      window.location.href = "/login"
    }
    throw new Error("Unauthorized")
  }

  if (!response.ok) {
    throw new Error(`API error ${response.status}: ${response.statusText}`)
  }

  return response.json() as Promise<T>
}
