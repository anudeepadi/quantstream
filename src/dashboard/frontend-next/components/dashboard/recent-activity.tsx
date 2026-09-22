"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"
import { useRecentActivity } from "@/lib/hooks/use-portfolio"
import { MOCK_RECENT_ACTIVITY } from "@/lib/mock-data/dashboard"
import {
  ArrowUpRight,
  ArrowDownRight,
  Coins,
  Wallet,
  LogOut,
  Search,
  MoreHorizontal,
} from "lucide-react"
import { formatCurrency } from "@/lib/utils/format"
import type { RecentActivity as RecentActivityType } from "@/lib/types/portfolio"

const TYPE_CONFIG: Record<
  RecentActivityType["type"],
  {
    icon: React.ElementType
    label: string
    badgeClass: string
  }
> = {
  buy: {
    icon: ArrowDownRight,
    label: "Buy",
    badgeClass: "bg-positive/10 text-positive border-positive/20",
  },
  sell: {
    icon: ArrowUpRight,
    label: "Sell",
    badgeClass: "bg-[oklch(0.58_0.150_250)]/10 text-[oklch(0.45_0.150_250)] border-[oklch(0.58_0.150_250)]/20",
  },
  dividend: {
    icon: Coins,
    label: "Dividend",
    badgeClass: "bg-[oklch(0.65_0.18_52)]/10 text-[oklch(0.50_0.18_52)] border-[oklch(0.65_0.18_52)]/20",
  },
  deposit: {
    icon: Wallet,
    label: "Deposit",
    badgeClass: "bg-positive/10 text-positive border-positive/20",
  },
  withdrawal: {
    icon: LogOut,
    label: "Withdrawal",
    badgeClass: "bg-negative/10 text-negative border-negative/20",
  },
}

function timeAgo(timestamp: string): string {
  const diff = Date.now() - new Date(timestamp).getTime()
  const hours = Math.floor(diff / 3_600_000)
  if (hours < 1) return "Just now"
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

export function RecentActivity() {
  const { data: activities } = useRecentActivity()
  const items = activities ?? MOCK_RECENT_ACTIVITY

  return (
    <Card>
      <CardHeader className="pb-0 pt-5 px-5">
        <div className="flex items-center justify-between gap-4">
          <div>
            <CardTitle className="text-[15px] font-semibold">Recent Activity</CardTitle>
            <p className="text-xs text-muted-foreground mt-0.5">
              Keep track of all portfolio transactions
            </p>
          </div>
          {/* Search + filter bar — like the reference's order list */}
          <div className="flex items-center gap-2 shrink-0">
            <div className="relative hidden sm:block">
              <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="Search..."
                className="pl-8 h-8 w-36 text-xs rounded-lg bg-muted border-transparent"
              />
            </div>
          </div>
        </div>

        {/* Column headers — table-like */}
        <div className="grid grid-cols-[auto_1fr_auto_auto_auto] gap-3 mt-4 pb-2 border-b border-border">
          <div className="w-7" />
          <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            Description
          </p>
          <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground text-center w-20">
            Type
          </p>
          <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground text-right w-20">
            Amount
          </p>
          <div className="w-6" />
        </div>
      </CardHeader>

      <CardContent className="px-5 pb-3 pt-0">
        <div className="divide-y divide-border/60">
          {items.map((activity) => {
            const config = TYPE_CONFIG[activity.type]
            const Icon = config.icon
            const isInflow = ["buy", "deposit", "dividend"].includes(activity.type)

            return (
              <div
                key={activity.id}
                className="grid grid-cols-[auto_1fr_auto_auto_auto] gap-3 items-center py-3 group"
              >
                {/* Icon */}
                <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-muted">
                  <Icon className="h-3.5 w-3.5 text-muted-foreground" />
                </div>

                {/* Description + time */}
                <div className="min-w-0">
                  <p className="text-[13px] font-medium truncate leading-tight">
                    {activity.description}
                  </p>
                  <p className="text-[11px] text-muted-foreground mt-0.5">
                    {timeAgo(activity.timestamp)}
                  </p>
                </div>

                {/* Type badge */}
                <div className="flex justify-center w-20">
                  <span
                    className={cn(
                      "inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold",
                      config.badgeClass
                    )}
                  >
                    {config.label}
                  </span>
                </div>

                {/* Amount */}
                <span
                  className={cn(
                    "text-[13px] font-semibold text-right w-20 shrink-0",
                    isInflow ? "text-positive" : "text-negative"
                  )}
                >
                  {isInflow ? "+" : "-"}{formatCurrency(Math.abs(activity.amount))}
                </span>

                {/* Action */}
                <button className="flex h-6 w-6 items-center justify-center rounded-md text-muted-foreground hover:text-foreground hover:bg-accent opacity-0 group-hover:opacity-100 transition-all">
                  <MoreHorizontal className="h-3.5 w-3.5" />
                </button>
              </div>
            )
          })}
        </div>
      </CardContent>
    </Card>
  )
}
