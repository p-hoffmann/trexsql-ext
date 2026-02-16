import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { Toaster } from "@/components/ui/sonner";
import { useSession } from "@/lib/auth-client";
import { GraphQLProvider } from "@/lib/graphql-client";
import { BASE_PATH } from "@/lib/config";
import { Layout } from "@/components/Layout";
import { AdminLayout } from "@/components/AdminLayout";
import { Login } from "@/pages/Login";
import { Register } from "@/pages/Register";
import { ForgotPassword } from "@/pages/ForgotPassword";
import { ResetPassword } from "@/pages/ResetPassword";
import { VerifyEmail } from "@/pages/VerifyEmail";
import { Consent } from "@/pages/Consent";
import { Profile } from "@/pages/Profile";
import { Users } from "@/pages/admin/Users";
import { UserDetail } from "@/pages/admin/UserDetail";
import { Apps } from "@/pages/admin/Apps";
import { AppDetail } from "@/pages/admin/AppDetail";
import { Databases } from "@/pages/admin/Databases";
import { DatabaseDetail } from "@/pages/admin/DatabaseDetail";
import { Sessions } from "@/pages/admin/Sessions";
import { Roles } from "@/pages/admin/Roles";
import { RoleDetail } from "@/pages/admin/RoleDetail";
import { Plugins } from "@/pages/admin/Plugins";
import { SsoProviders } from "@/pages/admin/SsoProviders";
import { Services } from "@/pages/admin/Services";
import { TrexDB } from "@/pages/admin/TrexDB";
import { Extensions } from "@/pages/admin/Extensions";

function HomeRedirect() {
  const { data: session, isPending } = useSession();
  if (isPending) return null;
  return <Navigate to={session ? "/profile" : "/login"} replace />;
}

export default function App() {
  return (
    <GraphQLProvider>
    <BrowserRouter basename={BASE_PATH}>
      <Routes>
        {/* Public routes */}
        <Route path="/login" element={<Login />} />
        <Route path="/register" element={<Register />} />
        <Route path="/forgot-password" element={<ForgotPassword />} />
        <Route path="/reset-password" element={<ResetPassword />} />
        <Route path="/verify-email" element={<VerifyEmail />} />
        <Route path="/consent" element={<Consent />} />

        {/* Authenticated routes */}
        <Route element={<Layout />}>
          <Route path="/profile" element={<Profile />} />

          {/* Admin routes */}
          <Route path="/admin" element={<AdminLayout />}>
            <Route index element={<Navigate to="users" replace />} />
            <Route path="users" element={<Users />} />
            <Route path="users/:id" element={<UserDetail />} />
            <Route path="apps" element={<Apps />} />
            <Route path="apps/:id" element={<AppDetail />} />
            <Route path="roles" element={<Roles />} />
            <Route path="roles/:id" element={<RoleDetail />} />
            <Route path="databases" element={<Databases />} />
            <Route path="databases/:id" element={<DatabaseDetail />} />
            <Route path="sessions" element={<Sessions />} />
            <Route path="plugins" element={<Plugins />} />
            <Route path="services" element={<Services />} />
            <Route path="trexdb" element={<TrexDB />} />
            <Route path="extensions" element={<Extensions />} />
            <Route path="sso" element={<SsoProviders />} />
          </Route>
        </Route>

        {/* Root redirect */}
        <Route path="/" element={<HomeRedirect />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
      <Toaster />
    </BrowserRouter>
    </GraphQLProvider>
  );
}
