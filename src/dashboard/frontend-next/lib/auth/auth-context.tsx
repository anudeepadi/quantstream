"use client"

import { createContext, useContext, useCallback, useEffect, useState } from "react"
import type { ReactNode } from "react"

interface User {
  readonly id: number
  readonly username: string
  readonly email: string
  readonly role: string
  readonly full_name: string | null
}

interface AuthState {
  readonly user: User | null
  readonly token: string | null
  readonly isLoading: boolean
  readonly isAuthenticated: boolean
  readonly login: (username: string, password: string) => Promise<void>
  readonly logout: () => Promise<void>
}

const AuthContext = createContext<AuthState | undefined>(undefined)

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000"

export function AuthProvider({ children }: { readonly children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [token, setToken] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  const clearSession = useCallback(() => {
    localStorage.removeItem("qs_token")
    localStorage.removeItem("qs_refresh")
    setToken(null)
    setUser(null)
  }, [])

  // Resolve the stored session before publishing authentication state.
  useEffect(() => {
    let cancelled = false
    const stored = localStorage.getItem("qs_token")
    const session = stored ? fetchMe(stored) : Promise.resolve(null)
    session.then((currentUser) => {
      if (cancelled) return
      if (currentUser && stored) {
        setToken(stored)
        setUser(currentUser)
      } else {
        clearSession()
      }
      setIsLoading(false)
    })
    return () => { cancelled = true }
  }, [clearSession])

  const login = useCallback(async (username: string, password: string) => {
    const res = await fetch(`${API_BASE}/api/v1/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    })

    if (!res.ok) {
      const body = await res.json().catch(() => ({ detail: "Login failed" }))
      throw new Error(body.detail ?? "Login failed")
    }

    const data = await res.json()
    localStorage.setItem("qs_token", data.access_token)
    localStorage.setItem("qs_refresh", data.refresh_token)
    setToken(data.access_token)

    const me = await fetchMe(data.access_token)
    if (me) setUser(me)
  }, [])

  const logout = useCallback(async () => {
    if (token) {
      await fetch(`${API_BASE}/api/v1/auth/logout`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
      }).catch(() => {})
    }
    clearSession()
  }, [token, clearSession])

  return (
    <AuthContext.Provider
      value={{
        user,
        token,
        isLoading,
        isAuthenticated: !!user,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth(): AuthState {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error("useAuth must be used within AuthProvider")
  return ctx
}

async function fetchMe(token: string): Promise<User | null> {
  try {
    const res = await fetch(`${API_BASE}/api/v1/auth/me`, {
      headers: { Authorization: `Bearer ${token}` },
    })
    if (!res.ok) return null
    return (await res.json()) as User
  } catch {
    return null
  }
}
