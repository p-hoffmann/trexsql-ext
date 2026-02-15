import { useSession } from "@/lib/auth-client";
import { Outlet, NavLink } from "react-router-dom";
import { Separator } from "@/components/ui/separator";

export function AdminLayout() {
  const { data: session } = useSession();

  if (session?.user && (session.user as any).role !== "admin") {
    return (
      <div className="flex items-center justify-center py-20">
        <div className="text-center">
          <h2 className="text-2xl font-bold">403 Forbidden</h2>
          <p className="text-muted-foreground mt-2">
            You do not have permission to access this page.
          </p>
        </div>
      </div>
    );
  }

  const navItems = [
    { to: "/admin/users", label: "Users" },
    { to: "/admin/roles", label: "Roles" },
    { to: "/admin/apps", label: "Applications" },
    { to: "/admin/databases", label: "Databases" },
    { to: "/admin/sessions", label: "Sessions" },
  ];

  return (
    <div className="flex gap-6">
      <aside className="w-48 shrink-0">
        <nav className="flex flex-col gap-1">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                `px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                  isActive
                    ? "bg-accent text-accent-foreground"
                    : "text-muted-foreground hover:bg-accent/50"
                }`
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <Separator orientation="vertical" className="h-auto" />
      <div className="flex-1 min-w-0">
        <Outlet />
      </div>
    </div>
  );
}
