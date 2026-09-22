import { Card, CardContent } from "@/components/ui/card"

export function CardSkeleton({ lines = 3 }: { readonly lines?: number }) {
  return (
    <Card>
      <CardContent className="p-5 animate-pulse">
        <div className="h-3 w-24 rounded bg-muted mb-4" />
        <div className="h-7 w-32 rounded bg-muted mb-3" />
        {Array.from({ length: lines }, (_, i) => (
          <div
            key={i}
            className="h-3 rounded bg-muted mb-2"
            style={{ width: `${[85, 100, 70][i % 3]}%` }}
          />
        ))}
      </CardContent>
    </Card>
  )
}

export function TableSkeleton({ rows = 5, cols = 4 }: { readonly rows?: number; readonly cols?: number }) {
  return (
    <Card>
      <CardContent className="p-5 animate-pulse">
        <div className="h-3 w-32 rounded bg-muted mb-5" />
        <div className="space-y-3">
          {Array.from({ length: rows }, (_, r) => (
            <div key={r} className="flex items-center gap-4">
              {Array.from({ length: cols }, (_, c) => (
                <div
                  key={c}
                  className="h-4 rounded bg-muted flex-1"
                  style={{ opacity: 0.4 + ((r + c) % 3) * 0.2 }}
                />
              ))}
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}
