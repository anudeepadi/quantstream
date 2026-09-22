"use client";

import { useTheme } from "next-themes";
import { useAuth } from "@/lib/auth/auth-context";
import { usePathname } from "next/navigation";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Search,
  Sun,
  Moon,
  Bell,
  User,
  LogOut,
  Settings,
  Download,
} from "lucide-react";

const PAGE_TITLES: Record<string, string> = {
  "/dashboard": "Dashboard",
  "/analytics": "Analytics",
  "/markets": "Live Markets",
  "/markets/watchlist": "Watchlist",
  "/markets/historical": "Historical Data",
  "/portfolio": "Portfolio",
  "/portfolio/pnl": "P&L Analysis",
  "/portfolio/allocation": "Allocation",
  "/analysis": "Technical Analysis",
  "/alerts": "Alerts",
  "/system": "System Metrics",
  "/settings": "Settings",
};

export function Header() {
  const { theme, setTheme } = useTheme();
  const { user, logout } = useAuth();
  const pathname = usePathname();

  const toggleTheme = () => {
    setTheme(theme === "dark" ? "light" : "dark");
  };

  const pageTitle = PAGE_TITLES[pathname] ?? "Dashboard";

  const initials = user?.full_name
    ? user.full_name
        .split(" ")
        .map((n) => n[0])
        .join("")
        .toUpperCase()
        .slice(0, 2)
    : (user?.username?.slice(0, 2).toUpperCase() ?? "QS");

  return (
    <header className="flex h-16 shrink-0 items-center justify-between border-b border-border bg-card px-6 gap-4">
      {/* Left: Page title */}
      <div className="shrink-0">
        <h2 className="text-[15px] font-semibold text-foreground leading-none">
          {pageTitle}
        </h2>
      </div>

      {/* Center: Search */}
      <div className="relative flex-1 max-w-sm">
        <Search className="absolute left-3.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        <Input
          placeholder="Search markets, symbols..."
          className="pl-9 pr-12 rounded-full bg-muted border-transparent text-sm h-9 focus-visible:ring-primary/30 focus-visible:border-primary/40 focus-visible:bg-background transition-all"
        />
        <kbd className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 rounded-md bg-background border border-border px-1.5 py-0.5 text-[10px] font-medium text-muted-foreground shadow-sm">
          ⌘K
        </kbd>
      </div>

      {/* Right: Actions */}
      <div className="flex items-center gap-1 shrink-0">
        {/* Export */}
        <Button
          variant="outline"
          size="sm"
          className="h-8 gap-1.5 text-xs font-medium rounded-lg hidden sm:flex"
        >
          <Download className="h-3.5 w-3.5" />
          Export
        </Button>

        {/* Theme toggle */}
        <Button
          variant="ghost"
          size="icon"
          onClick={toggleTheme}
          className="h-9 w-9 rounded-xl text-muted-foreground hover:text-foreground"
        >
          {theme === "dark" ? (
            <Sun className="h-4 w-4" />
          ) : (
            <Moon className="h-4 w-4" />
          )}
        </Button>

        {/* Notifications */}
        <Button
          variant="ghost"
          size="icon"
          className="h-9 w-9 rounded-xl relative text-muted-foreground hover:text-foreground"
        >
          <Bell className="h-4 w-4" />
          <span className="absolute top-1.5 right-1.5 h-2 w-2 rounded-full bg-primary ring-2 ring-card" />
        </Button>

        {/* User menu */}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="flex items-center gap-2 rounded-xl px-2 py-1.5 hover:bg-accent active:scale-[0.97] transition-all ml-1">
              <Avatar className="h-7 w-7">
                <AvatarFallback className="text-[10px] bg-primary/10 text-primary font-semibold">
                  {initials}
                </AvatarFallback>
              </Avatar>
              {user && (
                <div className="hidden sm:block text-left">
                  <p className="text-[13px] font-semibold leading-none">
                    {user.full_name?.split(" ")[0] ?? user.username}
                  </p>
                  <p className="text-[11px] text-muted-foreground leading-none mt-0.5 capitalize">
                    {user.role}
                  </p>
                </div>
              )}
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-48 rounded-xl">
            {user && (
              <div className="px-3 py-2 text-sm border-b border-border mb-1">
                <p className="font-semibold text-[13px]">
                  {user.full_name ?? user.username}
                </p>
                <p className="text-[11px] text-muted-foreground">
                  {user.email}
                </p>
              </div>
            )}
            <DropdownMenuItem className="rounded-lg">
              <User className="mr-2 h-4 w-4" />
              Profile
            </DropdownMenuItem>
            <DropdownMenuItem className="rounded-lg">
              <Settings className="mr-2 h-4 w-4" />
              Settings
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem
              onClick={logout}
              className="rounded-lg text-negative focus:text-negative"
            >
              <LogOut className="mr-2 h-4 w-4" />
              Log out
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </header>
  );
}
