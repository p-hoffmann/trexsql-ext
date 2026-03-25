// @ts-nocheck - Deno edge function
/**
 * App template registry and scaffolding.
 */

import { duckdb, escapeSql } from "./duckdb.ts";

interface AppTemplate {
  id: string;
  name: string;
  description: string;
  tech_stack: string;
  dev_command: string;
  install_command: string;
  build_command: string;
  /** Inline files to write for scaffolding */
  files: Record<string, string>;
}

export const TEMPLATES: AppTemplate[] = [
  {
    id: "react-vite",
    name: "React (Vite)",
    description: "React + Vite + TypeScript + Tailwind",
    tech_stack: "react",
    dev_command: "npm run dev",
    install_command: "npm install",
    build_command: "npm run build",
    files: {
      "AI_RULES.md": `# Tech Stack
- You are building a React application.
- Use TypeScript.
- Use React Router. KEEP the routes in src/App.tsx
- Always put source code in the src folder.
- Put pages into src/pages/
- Put components into src/components/
- The main page (default page) is src/pages/Index.tsx
- UPDATE the main page to include the new components. OTHERWISE, the user can NOT see any components!
- ALWAYS try to use the shadcn/ui library.
- Tailwind CSS: always use Tailwind CSS for styling components. Utilize Tailwind classes extensively for layout, spacing, colors, and other design aspects.

Available packages and libraries:
- The lucide-react package is installed for icons.
- You ALREADY have ALL the shadcn/ui components and their dependencies installed. So you don't need to install them again.
- You have ALL the necessary Radix UI components installed.
- Use prebuilt components from the shadcn/ui library after importing them. Note that these files shouldn't be edited, so make new components if you need to change them.
`,
      "package.json": `{
  "name": "my-react-app",
  "private": true,
  "version": "0.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "@radix-ui/react-accordion": "^1.2.0",
    "@radix-ui/react-alert-dialog": "^1.1.1",
    "@radix-ui/react-aspect-ratio": "^1.1.0",
    "@radix-ui/react-avatar": "^1.1.0",
    "@radix-ui/react-checkbox": "^1.1.1",
    "@radix-ui/react-collapsible": "^1.1.0",
    "@radix-ui/react-context-menu": "^2.2.1",
    "@radix-ui/react-dialog": "^1.1.2",
    "@radix-ui/react-dropdown-menu": "^2.1.1",
    "@radix-ui/react-hover-card": "^1.1.1",
    "@radix-ui/react-label": "^2.1.0",
    "@radix-ui/react-menubar": "^1.1.1",
    "@radix-ui/react-navigation-menu": "^1.2.0",
    "@radix-ui/react-popover": "^1.1.1",
    "@radix-ui/react-progress": "^1.1.0",
    "@radix-ui/react-radio-group": "^1.2.0",
    "@radix-ui/react-scroll-area": "^1.1.0",
    "@radix-ui/react-select": "^2.1.1",
    "@radix-ui/react-separator": "^1.1.0",
    "@radix-ui/react-slider": "^1.2.0",
    "@radix-ui/react-slot": "^1.1.0",
    "@radix-ui/react-switch": "^1.1.0",
    "@radix-ui/react-tabs": "^1.1.0",
    "@radix-ui/react-toast": "^1.2.1",
    "@radix-ui/react-toggle": "^1.1.0",
    "@radix-ui/react-toggle-group": "^1.1.0",
    "@radix-ui/react-tooltip": "^1.1.4",
    "class-variance-authority": "^0.7.1",
    "clsx": "^2.1.1",
    "cmdk": "^1.0.0",
    "date-fns": "^3.6.0",
    "embla-carousel-react": "^8.3.0",
    "input-otp": "^1.2.4",
    "lucide-react": "^0.462.0",
    "magic-string": "^0.30.17",
    "react": "^18.3.1",
    "react-day-picker": "^9.13.0",
    "react-dom": "^18.3.1",
    "react-hook-form": "^7.53.0",
    "react-resizable-panels": "^2.1.3",
    "react-router-dom": "^6.26.2",
    "recharts": "^2.12.7",
    "sonner": "^1.5.0",
    "tailwind-merge": "^2.5.2",
    "tailwindcss-animate": "^1.0.7",
    "vaul": "^0.9.3",
    "zod": "^3.23.8"
  },
  "devDependencies": {
    "@types/node": "^22.5.5",
    "@types/react": "^18.3.12",
    "@types/react-dom": "^18.3.1",
    "@vitejs/plugin-react": "^4.3.4",
    "autoprefixer": "^10.4.20",
    "postcss": "^8.4.47",
    "tailwindcss": "^3.4.11",
    "typescript": "~5.6.2",
    "vite": "^6.0.1"
  }
}`,
      "vite.config.ts": `import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
})`,
      "tsconfig.json": `{
  "files": [],
  "references": [
    { "path": "./tsconfig.app.json" },
    { "path": "./tsconfig.node.json" }
  ]
}`,
      "tsconfig.app.json": `{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "isolatedModules": true,
    "moduleDetection": "force",
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "noUncheckedSideEffectImports": true,
    "baseUrl": ".",
    "paths": { "@/*": ["./src/*"] }
  },
  "include": ["src"]
}`,
      "tsconfig.node.json": `{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2023"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "isolatedModules": true,
    "moduleDetection": "force",
    "noEmit": true,
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "noUncheckedSideEffectImports": true
  },
  "include": ["vite.config.ts"]
}`,
      "postcss.config.js": `export default {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
};`,
      "tailwind.config.ts": `import type { Config } from "tailwindcss";

export default {
  darkMode: ["class"],
  content: [
    "./pages/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./app/**/*.{ts,tsx}",
    "./src/**/*.{ts,tsx}",
  ],
  prefix: "",
  theme: {
    container: {
      center: true,
      padding: "2rem",
      screens: {
        "2xl": "1400px",
      },
    },
    extend: {
      colors: {
        border: "hsl(var(--border))",
        input: "hsl(var(--input))",
        ring: "hsl(var(--ring))",
        background: "hsl(var(--background))",
        foreground: "hsl(var(--foreground))",
        primary: {
          DEFAULT: "hsl(var(--primary))",
          foreground: "hsl(var(--primary-foreground))",
        },
        secondary: {
          DEFAULT: "hsl(var(--secondary))",
          foreground: "hsl(var(--secondary-foreground))",
        },
        destructive: {
          DEFAULT: "hsl(var(--destructive))",
          foreground: "hsl(var(--destructive-foreground))",
        },
        muted: {
          DEFAULT: "hsl(var(--muted))",
          foreground: "hsl(var(--muted-foreground))",
        },
        accent: {
          DEFAULT: "hsl(var(--accent))",
          foreground: "hsl(var(--accent-foreground))",
        },
        popover: {
          DEFAULT: "hsl(var(--popover))",
          foreground: "hsl(var(--popover-foreground))",
        },
        card: {
          DEFAULT: "hsl(var(--card))",
          foreground: "hsl(var(--card-foreground))",
        },
      },
      borderRadius: {
        lg: "var(--radius)",
        md: "calc(var(--radius) - 2px)",
        sm: "calc(var(--radius) - 4px)",
      },
      keyframes: {
        "accordion-down": {
          from: { height: "0" },
          to: { height: "var(--radix-accordion-content-height)" },
        },
        "accordion-up": {
          from: { height: "var(--radix-accordion-content-height)" },
          to: { height: "0" },
        },
      },
      animation: {
        "accordion-down": "accordion-down 0.2s ease-out",
        "accordion-up": "accordion-up 0.2s ease-out",
      },
    },
  },
  plugins: [require("tailwindcss-animate")],
} satisfies Config;`,
      "components.json": `{
  "$schema": "https://ui.shadcn.com/schema.json",
  "style": "default",
  "rsc": false,
  "tsx": true,
  "tailwind": {
    "config": "tailwind.config.ts",
    "css": "src/globals.css",
    "baseColor": "slate",
    "cssVariables": true,
    "prefix": ""
  },
  "aliases": {
    "components": "@/components",
    "utils": "@/lib/utils",
    "ui": "@/components/ui",
    "lib": "@/lib",
    "hooks": "@/hooks"
  }
}`,
      "index.html": `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>React App</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>`,
      "src/main.tsx": `import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./globals.css";

createRoot(document.getElementById("root")!).render(<App />);`,
      "src/App.tsx": `import { BrowserRouter, Routes, Route } from "react-router-dom";
import Index from "./pages/Index";
import NotFound from "./pages/NotFound";

const App = () => (
  <BrowserRouter basename={import.meta.env.BASE_URL}>
    <Routes>
      <Route path="/" element={<Index />} />
      {/* ADD ALL CUSTOM ROUTES ABOVE THE CATCH-ALL "*" ROUTE */}
      <Route path="*" element={<NotFound />} />
    </Routes>
  </BrowserRouter>
);

export default App;`,
      "src/pages/Index.tsx": `const Index = () => {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100">
      <div className="text-center">
        <h1 className="text-4xl font-bold mb-4">Welcome to Your App</h1>
        <p className="text-xl text-gray-600">
          Start building your amazing project here!
        </p>
      </div>
    </div>
  );
};

export default Index;`,
      "src/pages/NotFound.tsx": `import { useLocation } from "react-router-dom";
import { useEffect } from "react";

const NotFound = () => {
  const location = useLocation();

  useEffect(() => {
    console.error(
      "404 Error: User attempted to access non-existent route:",
      location.pathname,
    );
  }, [location.pathname]);

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100">
      <div className="text-center">
        <h1 className="text-4xl font-bold mb-4">404</h1>
        <p className="text-xl text-gray-600 mb-4">Oops! Page not found</p>
        <a href="/" className="text-blue-500 hover:text-blue-700 underline">
          Return to Home
        </a>
      </div>
    </div>
  );
};

export default NotFound;`,
      "src/globals.css": `@tailwind base;
@tailwind components;
@tailwind utilities;

@layer base {
  :root {
    --background: 0 0% 100%;
    --foreground: 222.2 84% 4.9%;
    --card: 0 0% 100%;
    --card-foreground: 222.2 84% 4.9%;
    --popover: 0 0% 100%;
    --popover-foreground: 222.2 84% 4.9%;
    --primary: 222.2 47.4% 11.2%;
    --primary-foreground: 210 40% 98%;
    --secondary: 210 40% 96.1%;
    --secondary-foreground: 222.2 47.4% 11.2%;
    --muted: 210 40% 96.1%;
    --muted-foreground: 215.4 16.3% 46.9%;
    --accent: 210 40% 96.1%;
    --accent-foreground: 222.2 47.4% 11.2%;
    --destructive: 0 84.2% 60.2%;
    --destructive-foreground: 210 40% 98%;
    --border: 214.3 31.8% 91.4%;
    --input: 214.3 31.8% 91.4%;
    --ring: 222.2 84% 4.9%;
    --radius: 0.5rem;
  }

  .dark {
    --background: 222.2 84% 4.9%;
    --foreground: 210 40% 98%;
    --card: 222.2 84% 4.9%;
    --card-foreground: 210 40% 98%;
    --popover: 222.2 84% 4.9%;
    --popover-foreground: 210 40% 98%;
    --primary: 210 40% 98%;
    --primary-foreground: 222.2 47.4% 11.2%;
    --secondary: 217.2 32.6% 17.5%;
    --secondary-foreground: 210 40% 98%;
    --muted: 217.2 32.6% 17.5%;
    --muted-foreground: 215 20.2% 65.1%;
    --accent: 217.2 32.6% 17.5%;
    --accent-foreground: 210 40% 98%;
    --destructive: 0 62.8% 30.6%;
    --destructive-foreground: 210 40% 98%;
    --border: 217.2 32.6% 17.5%;
    --input: 217.2 32.6% 17.5%;
    --ring: 212.7 26.8% 83.9%;
  }
}

@layer base {
  * {
    @apply border-border;
  }

  body {
    @apply bg-background text-foreground;
  }
}`,
      "src/lib/utils.ts": `import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}`,
      "src/hooks/use-toast.ts": `import * as React from "react";

const TOAST_LIMIT = 1;
const TOAST_REMOVE_DELAY = 1000000;

type ToasterToast = {
  id: string;
  title?: string;
  description?: string;
  action?: React.ReactNode;
  variant?: "default" | "destructive";
};

let count = 0;
function genId() { return (count = (count + 1) % Number.MAX_SAFE_INTEGER).toString(); }

type State = { toasts: ToasterToast[] };
type Action =
  | { type: "ADD_TOAST"; toast: ToasterToast }
  | { type: "UPDATE_TOAST"; toast: Partial<ToasterToast> }
  | { type: "DISMISS_TOAST"; toastId?: string }
  | { type: "REMOVE_TOAST"; toastId?: string };

const listeners: Array<(s: State) => void> = [];
let memoryState: State = { toasts: [] };

function dispatch(action: Action) {
  memoryState = reducer(memoryState, action);
  listeners.forEach((l) => l(memoryState));
}

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case "ADD_TOAST":
      return { ...state, toasts: [action.toast, ...state.toasts].slice(0, TOAST_LIMIT) };
    case "UPDATE_TOAST":
      return { ...state, toasts: state.toasts.map((t) => (t.id === action.toast.id ? { ...t, ...action.toast } : t)) };
    case "DISMISS_TOAST":
      return { ...state, toasts: state.toasts.map((t) => (action.toastId == null || t.id === action.toastId ? { ...t } : t)) };
    case "REMOVE_TOAST":
      if (action.toastId == null) return { ...state, toasts: [] };
      return { ...state, toasts: state.toasts.filter((t) => t.id !== action.toastId) };
  }
}

function toast(props: Omit<ToasterToast, "id">) {
  const id = genId();
  dispatch({ type: "ADD_TOAST", toast: { ...props, id } });
  return { id, dismiss: () => dispatch({ type: "DISMISS_TOAST", toastId: id }), update: (p: Partial<ToasterToast>) => dispatch({ type: "UPDATE_TOAST", toast: { ...p, id } }) };
}

function useToast() {
  const [state, setState] = React.useState<State>(memoryState);
  React.useEffect(() => { listeners.push(setState); return () => { const i = listeners.indexOf(setState); if (i > -1) listeners.splice(i, 1); }; }, []);
  return { ...state, toast, dismiss: (toastId?: string) => dispatch({ type: "DISMISS_TOAST", toastId }) };
}

export { useToast, toast };
export type { ToasterToast };`,
      "src/components/ui/button.tsx": `import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const buttonVariants = cva(
  "inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0",
  {
    variants: {
      variant: {
        default: "bg-primary text-primary-foreground hover:bg-primary/90",
        destructive: "bg-destructive text-destructive-foreground hover:bg-destructive/90",
        outline: "border border-input bg-background hover:bg-accent hover:text-accent-foreground",
        secondary: "bg-secondary text-secondary-foreground hover:bg-secondary/80",
        ghost: "hover:bg-accent hover:text-accent-foreground",
        link: "text-primary underline-offset-4 hover:underline",
      },
      size: {
        default: "h-10 px-4 py-2",
        sm: "h-9 rounded-md px-3",
        lg: "h-11 rounded-md px-8",
        icon: "h-10 w-10",
      },
    },
    defaultVariants: { variant: "default", size: "default" },
  }
);

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof buttonVariants> {
  asChild?: boolean;
}

const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(({ className, variant, size, asChild = false, ...props }, ref) => {
  const Comp = asChild ? Slot : "button";
  return <Comp className={cn(buttonVariants({ variant, size, className }))} ref={ref} {...props} />;
});
Button.displayName = "Button";

export { Button, buttonVariants };`,
      "src/components/ui/input.tsx": `import * as React from "react";
import { cn } from "@/lib/utils";

const Input = React.forwardRef<HTMLInputElement, React.InputHTMLAttributes<HTMLInputElement>>(({ className, type, ...props }, ref) => {
  return (
    <input
      type={type}
      className={cn("flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-base ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium file:text-foreground placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 md:text-sm", className)}
      ref={ref}
      {...props}
    />
  );
});
Input.displayName = "Input";

export { Input };`,
      "src/components/ui/label.tsx": `import * as React from "react";
import * as LabelPrimitive from "@radix-ui/react-label";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const labelVariants = cva("text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70");

const Label = React.forwardRef<
  React.ElementRef<typeof LabelPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof LabelPrimitive.Root> & VariantProps<typeof labelVariants>
>(({ className, ...props }, ref) => (
  <LabelPrimitive.Root ref={ref} className={cn(labelVariants(), className)} {...props} />
));
Label.displayName = LabelPrimitive.Root.displayName;

export { Label };`,
      "src/components/ui/badge.tsx": `import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2",
  {
    variants: {
      variant: {
        default: "border-transparent bg-primary text-primary-foreground hover:bg-primary/80",
        secondary: "border-transparent bg-secondary text-secondary-foreground hover:bg-secondary/80",
        destructive: "border-transparent bg-destructive text-destructive-foreground hover:bg-destructive/80",
        outline: "text-foreground",
      },
    },
    defaultVariants: { variant: "default" },
  }
);

export interface BadgeProps extends React.HTMLAttributes<HTMLDivElement>, VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />;
}

export { Badge, badgeVariants };`,
      "src/components/ui/card.tsx": `import * as React from "react";
import { cn } from "@/lib/utils";

const Card = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(({ className, ...props }, ref) => (
  <div ref={ref} className={cn("rounded-lg border bg-card text-card-foreground shadow-sm", className)} {...props} />
));
Card.displayName = "Card";

const CardHeader = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(({ className, ...props }, ref) => (
  <div ref={ref} className={cn("flex flex-col space-y-1.5 p-6", className)} {...props} />
));
CardHeader.displayName = "CardHeader";

const CardTitle = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(({ className, ...props }, ref) => (
  <div ref={ref} className={cn("text-2xl font-semibold leading-none tracking-tight", className)} {...props} />
));
CardTitle.displayName = "CardTitle";

const CardDescription = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(({ className, ...props }, ref) => (
  <div ref={ref} className={cn("text-sm text-muted-foreground", className)} {...props} />
));
CardDescription.displayName = "CardDescription";

const CardContent = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(({ className, ...props }, ref) => (
  <div ref={ref} className={cn("p-6 pt-0", className)} {...props} />
));
CardContent.displayName = "CardContent";

const CardFooter = React.forwardRef<HTMLDivElement, React.HTMLAttributes<HTMLDivElement>>(({ className, ...props }, ref) => (
  <div ref={ref} className={cn("flex items-center p-6 pt-0", className)} {...props} />
));
CardFooter.displayName = "CardFooter";

export { Card, CardHeader, CardFooter, CardTitle, CardDescription, CardContent };`,
      "src/components/ui/textarea.tsx": `import * as React from "react";
import { cn } from "@/lib/utils";

const Textarea = React.forwardRef<HTMLTextAreaElement, React.TextareaHTMLAttributes<HTMLTextAreaElement>>(({ className, ...props }, ref) => {
  return (
    <textarea
      className={cn("flex min-h-[80px] w-full rounded-md border border-input bg-background px-3 py-2 text-base ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 md:text-sm", className)}
      ref={ref}
      {...props}
    />
  );
});
Textarea.displayName = "Textarea";

export { Textarea };`,
      "src/components/ui/separator.tsx": `import * as React from "react";
import * as SeparatorPrimitive from "@radix-ui/react-separator";
import { cn } from "@/lib/utils";

const Separator = React.forwardRef<
  React.ElementRef<typeof SeparatorPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof SeparatorPrimitive.Root>
>(({ className, orientation = "horizontal", decorative = true, ...props }, ref) => (
  <SeparatorPrimitive.Root
    ref={ref}
    decorative={decorative}
    orientation={orientation}
    className={cn("shrink-0 bg-border", orientation === "horizontal" ? "h-[1px] w-full" : "h-full w-[1px]", className)}
    {...props}
  />
));
Separator.displayName = SeparatorPrimitive.Root.displayName;

export { Separator };`,
      "src/components/ui/checkbox.tsx": `import * as React from "react";
import * as CheckboxPrimitive from "@radix-ui/react-checkbox";
import { Check } from "lucide-react";
import { cn } from "@/lib/utils";

const Checkbox = React.forwardRef<
  React.ElementRef<typeof CheckboxPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof CheckboxPrimitive.Root>
>(({ className, ...props }, ref) => (
  <CheckboxPrimitive.Root
    ref={ref}
    className={cn("peer h-4 w-4 shrink-0 rounded-sm border border-primary ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 data-[state=checked]:bg-primary data-[state=checked]:text-primary-foreground", className)}
    {...props}
  >
    <CheckboxPrimitive.Indicator className={cn("flex items-center justify-center text-current")}>
      <Check className="h-4 w-4" />
    </CheckboxPrimitive.Indicator>
  </CheckboxPrimitive.Root>
));
Checkbox.displayName = CheckboxPrimitive.Root.displayName;

export { Checkbox };`,
      "src/components/ui/switch.tsx": `import * as React from "react";
import * as SwitchPrimitives from "@radix-ui/react-switch";
import { cn } from "@/lib/utils";

const Switch = React.forwardRef<
  React.ElementRef<typeof SwitchPrimitives.Root>,
  React.ComponentPropsWithoutRef<typeof SwitchPrimitives.Root>
>(({ className, ...props }, ref) => (
  <SwitchPrimitives.Root
    className={cn("peer inline-flex h-6 w-11 shrink-0 cursor-pointer items-center rounded-full border-2 border-transparent transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background disabled:cursor-not-allowed disabled:opacity-50 data-[state=checked]:bg-primary data-[state=unchecked]:bg-input", className)}
    {...props}
    ref={ref}
  >
    <SwitchPrimitives.Thumb className={cn("pointer-events-none block h-5 w-5 rounded-full bg-background shadow-lg ring-0 transition-transform data-[state=checked]:translate-x-5 data-[state=unchecked]:translate-x-0")} />
  </SwitchPrimitives.Root>
));
Switch.displayName = SwitchPrimitives.Root.displayName;

export { Switch };`,
      "src/components/ui/select.tsx": `import * as React from "react";
import * as SelectPrimitive from "@radix-ui/react-select";
import { Check, ChevronDown, ChevronUp } from "lucide-react";
import { cn } from "@/lib/utils";

const Select = SelectPrimitive.Root;
const SelectGroup = SelectPrimitive.Group;
const SelectValue = SelectPrimitive.Value;

const SelectTrigger = React.forwardRef<
  React.ElementRef<typeof SelectPrimitive.Trigger>,
  React.ComponentPropsWithoutRef<typeof SelectPrimitive.Trigger>
>(({ className, children, ...props }, ref) => (
  <SelectPrimitive.Trigger ref={ref} className={cn("flex h-10 w-full items-center justify-between rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 [&>span]:line-clamp-1", className)} {...props}>
    {children}
    <SelectPrimitive.Icon asChild><ChevronDown className="h-4 w-4 opacity-50" /></SelectPrimitive.Icon>
  </SelectPrimitive.Trigger>
));
SelectTrigger.displayName = SelectPrimitive.Trigger.displayName;

const SelectContent = React.forwardRef<
  React.ElementRef<typeof SelectPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof SelectPrimitive.Content>
>(({ className, children, position = "popper", ...props }, ref) => (
  <SelectPrimitive.Portal>
    <SelectPrimitive.Content ref={ref} className={cn("relative z-50 max-h-96 min-w-[8rem] overflow-hidden rounded-md border bg-popover text-popover-foreground shadow-md data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2", position === "popper" && "data-[side=bottom]:translate-y-1 data-[side=left]:-translate-x-1 data-[side=right]:translate-x-1 data-[side=top]:-translate-y-1", className)} position={position} {...props}>
      <SelectPrimitive.ScrollUpButton className="flex cursor-default items-center justify-center py-1"><ChevronUp className="h-4 w-4" /></SelectPrimitive.ScrollUpButton>
      <SelectPrimitive.Viewport className={cn("p-1", position === "popper" && "h-[var(--radix-select-trigger-height)] w-full min-w-[var(--radix-select-trigger-width)]")}>{children}</SelectPrimitive.Viewport>
      <SelectPrimitive.ScrollDownButton className="flex cursor-default items-center justify-center py-1"><ChevronDown className="h-4 w-4" /></SelectPrimitive.ScrollDownButton>
    </SelectPrimitive.Content>
  </SelectPrimitive.Portal>
));
SelectContent.displayName = SelectPrimitive.Content.displayName;

const SelectItem = React.forwardRef<
  React.ElementRef<typeof SelectPrimitive.Item>,
  React.ComponentPropsWithoutRef<typeof SelectPrimitive.Item>
>(({ className, children, ...props }, ref) => (
  <SelectPrimitive.Item ref={ref} className={cn("relative flex w-full cursor-default select-none items-center rounded-sm py-1.5 pl-8 pr-2 text-sm outline-none focus:bg-accent focus:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50", className)} {...props}>
    <span className="absolute left-2 flex h-3.5 w-3.5 items-center justify-center"><SelectPrimitive.ItemIndicator><Check className="h-4 w-4" /></SelectPrimitive.ItemIndicator></span>
    <SelectPrimitive.ItemText>{children}</SelectPrimitive.ItemText>
  </SelectPrimitive.Item>
));
SelectItem.displayName = SelectPrimitive.Item.displayName;

const SelectLabel = React.forwardRef<
  React.ElementRef<typeof SelectPrimitive.Label>,
  React.ComponentPropsWithoutRef<typeof SelectPrimitive.Label>
>(({ className, ...props }, ref) => (
  <SelectPrimitive.Label ref={ref} className={cn("py-1.5 pl-8 pr-2 text-sm font-semibold", className)} {...props} />
));
SelectLabel.displayName = SelectPrimitive.Label.displayName;

const SelectSeparator = React.forwardRef<
  React.ElementRef<typeof SelectPrimitive.Separator>,
  React.ComponentPropsWithoutRef<typeof SelectPrimitive.Separator>
>(({ className, ...props }, ref) => (
  <SelectPrimitive.Separator ref={ref} className={cn("-mx-1 my-1 h-px bg-muted", className)} {...props} />
));
SelectSeparator.displayName = SelectPrimitive.Separator.displayName;

export { Select, SelectGroup, SelectValue, SelectTrigger, SelectContent, SelectItem, SelectLabel, SelectSeparator };`,
      "src/components/ui/dialog.tsx": `import * as React from "react";
import * as DialogPrimitive from "@radix-ui/react-dialog";
import { X } from "lucide-react";
import { cn } from "@/lib/utils";

const Dialog = DialogPrimitive.Root;
const DialogTrigger = DialogPrimitive.Trigger;
const DialogPortal = DialogPrimitive.Portal;
const DialogClose = DialogPrimitive.Close;

const DialogOverlay = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Overlay>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Overlay>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Overlay ref={ref} className={cn("fixed inset-0 z-50 bg-black/80 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0", className)} {...props} />
));
DialogOverlay.displayName = DialogPrimitive.Overlay.displayName;

const DialogContent = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Content>
>(({ className, children, ...props }, ref) => (
  <DialogPortal>
    <DialogOverlay />
    <DialogPrimitive.Content ref={ref} className={cn("fixed left-[50%] top-[50%] z-50 grid w-full max-w-lg translate-x-[-50%] translate-y-[-50%] gap-4 border bg-background p-6 shadow-lg duration-200 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[state=closed]:slide-out-to-left-1/2 data-[state=closed]:slide-out-to-top-[48%] data-[state=open]:slide-in-from-left-1/2 data-[state=open]:slide-in-from-top-[48%] sm:rounded-lg", className)} {...props}>
      {children}
      <DialogPrimitive.Close className="absolute right-4 top-4 rounded-sm opacity-70 ring-offset-background transition-opacity hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:pointer-events-none data-[state=open]:bg-accent data-[state=open]:text-muted-foreground">
        <X className="h-4 w-4" /><span className="sr-only">Close</span>
      </DialogPrimitive.Close>
    </DialogPrimitive.Content>
  </DialogPortal>
));
DialogContent.displayName = DialogPrimitive.Content.displayName;

const DialogHeader = ({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
  <div className={cn("flex flex-col space-y-1.5 text-center sm:text-left", className)} {...props} />
);
DialogHeader.displayName = "DialogHeader";

const DialogFooter = ({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
  <div className={cn("flex flex-col-reverse sm:flex-row sm:justify-end sm:space-x-2", className)} {...props} />
);
DialogFooter.displayName = "DialogFooter";

const DialogTitle = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Title>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Title>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Title ref={ref} className={cn("text-lg font-semibold leading-none tracking-tight", className)} {...props} />
));
DialogTitle.displayName = DialogPrimitive.Title.displayName;

const DialogDescription = React.forwardRef<
  React.ElementRef<typeof DialogPrimitive.Description>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Description>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Description ref={ref} className={cn("text-sm text-muted-foreground", className)} {...props} />
));
DialogDescription.displayName = DialogPrimitive.Description.displayName;

export { Dialog, DialogPortal, DialogOverlay, DialogContent, DialogHeader, DialogFooter, DialogTitle, DialogDescription, DialogTrigger, DialogClose };`,
      "src/components/ui/tabs.tsx": `import * as React from "react";
import * as TabsPrimitive from "@radix-ui/react-tabs";
import { cn } from "@/lib/utils";

const Tabs = TabsPrimitive.Root;

const TabsList = React.forwardRef<
  React.ElementRef<typeof TabsPrimitive.List>,
  React.ComponentPropsWithoutRef<typeof TabsPrimitive.List>
>(({ className, ...props }, ref) => (
  <TabsPrimitive.List ref={ref} className={cn("inline-flex h-10 items-center justify-center rounded-md bg-muted p-1 text-muted-foreground", className)} {...props} />
));
TabsList.displayName = TabsPrimitive.List.displayName;

const TabsTrigger = React.forwardRef<
  React.ElementRef<typeof TabsPrimitive.Trigger>,
  React.ComponentPropsWithoutRef<typeof TabsPrimitive.Trigger>
>(({ className, ...props }, ref) => (
  <TabsPrimitive.Trigger ref={ref} className={cn("inline-flex items-center justify-center whitespace-nowrap rounded-sm px-3 py-1.5 text-sm font-medium ring-offset-background transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 data-[state=active]:bg-background data-[state=active]:text-foreground data-[state=active]:shadow-sm", className)} {...props} />
));
TabsTrigger.displayName = TabsPrimitive.Trigger.displayName;

const TabsContent = React.forwardRef<
  React.ElementRef<typeof TabsPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof TabsPrimitive.Content>
>(({ className, ...props }, ref) => (
  <TabsPrimitive.Content ref={ref} className={cn("mt-2 ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2", className)} {...props} />
));
TabsContent.displayName = TabsPrimitive.Content.displayName;

export { Tabs, TabsList, TabsTrigger, TabsContent };`,
      "src/components/ui/scroll-area.tsx": `import * as React from "react";
import * as ScrollAreaPrimitive from "@radix-ui/react-scroll-area";
import { cn } from "@/lib/utils";

const ScrollArea = React.forwardRef<
  React.ElementRef<typeof ScrollAreaPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof ScrollAreaPrimitive.Root>
>(({ className, children, ...props }, ref) => (
  <ScrollAreaPrimitive.Root ref={ref} className={cn("relative overflow-hidden", className)} {...props}>
    <ScrollAreaPrimitive.Viewport className="h-full w-full rounded-[inherit]">{children}</ScrollAreaPrimitive.Viewport>
    <ScrollBar />
    <ScrollAreaPrimitive.Corner />
  </ScrollAreaPrimitive.Root>
));
ScrollArea.displayName = ScrollAreaPrimitive.Root.displayName;

const ScrollBar = React.forwardRef<
  React.ElementRef<typeof ScrollAreaPrimitive.ScrollAreaScrollbar>,
  React.ComponentPropsWithoutRef<typeof ScrollAreaPrimitive.ScrollAreaScrollbar>
>(({ className, orientation = "vertical", ...props }, ref) => (
  <ScrollAreaPrimitive.ScrollAreaScrollbar ref={ref} orientation={orientation} className={cn("flex touch-none select-none transition-colors", orientation === "vertical" && "h-full w-2.5 border-l border-l-transparent p-[1px]", orientation === "horizontal" && "h-2.5 flex-col border-t border-t-transparent p-[1px]", className)} {...props}>
    <ScrollAreaPrimitive.ScrollAreaThumb className="relative flex-1 rounded-full bg-border" />
  </ScrollAreaPrimitive.ScrollAreaScrollbar>
));
ScrollBar.displayName = ScrollAreaPrimitive.ScrollAreaScrollbar.displayName;

export { ScrollArea, ScrollBar };`,
      "src/components/ui/tooltip.tsx": `import * as React from "react";
import * as TooltipPrimitive from "@radix-ui/react-tooltip";
import { cn } from "@/lib/utils";

const TooltipProvider = TooltipPrimitive.Provider;
const Tooltip = TooltipPrimitive.Root;
const TooltipTrigger = TooltipPrimitive.Trigger;

const TooltipContent = React.forwardRef<
  React.ElementRef<typeof TooltipPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof TooltipPrimitive.Content>
>(({ className, sideOffset = 4, ...props }, ref) => (
  <TooltipPrimitive.Content ref={ref} sideOffset={sideOffset} className={cn("z-50 overflow-hidden rounded-md border bg-popover px-3 py-1.5 text-sm text-popover-foreground shadow-md animate-in fade-in-0 zoom-in-95 data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2", className)} {...props} />
));
TooltipContent.displayName = TooltipPrimitive.Content.displayName;

export { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider };`,
      "src/components/ui/avatar.tsx": `import * as React from "react";
import * as AvatarPrimitive from "@radix-ui/react-avatar";
import { cn } from "@/lib/utils";

const Avatar = React.forwardRef<
  React.ElementRef<typeof AvatarPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof AvatarPrimitive.Root>
>(({ className, ...props }, ref) => (
  <AvatarPrimitive.Root ref={ref} className={cn("relative flex h-10 w-10 shrink-0 overflow-hidden rounded-full", className)} {...props} />
));
Avatar.displayName = AvatarPrimitive.Root.displayName;

const AvatarImage = React.forwardRef<
  React.ElementRef<typeof AvatarPrimitive.Image>,
  React.ComponentPropsWithoutRef<typeof AvatarPrimitive.Image>
>(({ className, ...props }, ref) => (
  <AvatarPrimitive.Image ref={ref} className={cn("aspect-square h-full w-full", className)} {...props} />
));
AvatarImage.displayName = AvatarPrimitive.Image.displayName;

const AvatarFallback = React.forwardRef<
  React.ElementRef<typeof AvatarPrimitive.Fallback>,
  React.ComponentPropsWithoutRef<typeof AvatarPrimitive.Fallback>
>(({ className, ...props }, ref) => (
  <AvatarPrimitive.Fallback ref={ref} className={cn("flex h-full w-full items-center justify-center rounded-full bg-muted", className)} {...props} />
));
AvatarFallback.displayName = AvatarPrimitive.Fallback.displayName;

export { Avatar, AvatarImage, AvatarFallback };`,
      "src/components/ui/progress.tsx": `import * as React from "react";
import * as ProgressPrimitive from "@radix-ui/react-progress";
import { cn } from "@/lib/utils";

const Progress = React.forwardRef<
  React.ElementRef<typeof ProgressPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof ProgressPrimitive.Root>
>(({ className, value, ...props }, ref) => (
  <ProgressPrimitive.Root ref={ref} className={cn("relative h-4 w-full overflow-hidden rounded-full bg-secondary", className)} {...props}>
    <ProgressPrimitive.Indicator className="h-full w-full flex-1 bg-primary transition-all" style={{ transform: \`translateX(-\${100 - (value || 0)}%)\` }} />
  </ProgressPrimitive.Root>
));
Progress.displayName = ProgressPrimitive.Root.displayName;

export { Progress };`,
      "src/components/ui/slider.tsx": `import * as React from "react";
import * as SliderPrimitive from "@radix-ui/react-slider";
import { cn } from "@/lib/utils";

const Slider = React.forwardRef<
  React.ElementRef<typeof SliderPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof SliderPrimitive.Root>
>(({ className, ...props }, ref) => (
  <SliderPrimitive.Root ref={ref} className={cn("relative flex w-full touch-none select-none items-center", className)} {...props}>
    <SliderPrimitive.Track className="relative h-2 w-full grow overflow-hidden rounded-full bg-secondary">
      <SliderPrimitive.Range className="absolute h-full bg-primary" />
    </SliderPrimitive.Track>
    <SliderPrimitive.Thumb className="block h-5 w-5 rounded-full border-2 border-primary bg-background ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50" />
  </SliderPrimitive.Root>
));
Slider.displayName = SliderPrimitive.Root.displayName;

export { Slider };`,
      "src/components/ui/dropdown-menu.tsx": `import * as React from "react";
import * as DropdownMenuPrimitive from "@radix-ui/react-dropdown-menu";
import { Check, ChevronRight, Circle } from "lucide-react";
import { cn } from "@/lib/utils";

const DropdownMenu = DropdownMenuPrimitive.Root;
const DropdownMenuTrigger = DropdownMenuPrimitive.Trigger;
const DropdownMenuGroup = DropdownMenuPrimitive.Group;
const DropdownMenuPortal = DropdownMenuPrimitive.Portal;
const DropdownMenuSub = DropdownMenuPrimitive.Sub;
const DropdownMenuRadioGroup = DropdownMenuPrimitive.RadioGroup;

const DropdownMenuSubTrigger = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.SubTrigger>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.SubTrigger> & { inset?: boolean }
>(({ className, inset, children, ...props }, ref) => (
  <DropdownMenuPrimitive.SubTrigger ref={ref} className={cn("flex cursor-default gap-2 select-none items-center rounded-sm px-2 py-1.5 text-sm outline-none focus:bg-accent data-[state=open]:bg-accent [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0", inset && "pl-8", className)} {...props}>
    {children}<ChevronRight className="ml-auto" />
  </DropdownMenuPrimitive.SubTrigger>
));
DropdownMenuSubTrigger.displayName = DropdownMenuPrimitive.SubTrigger.displayName;

const DropdownMenuSubContent = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.SubContent>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.SubContent>
>(({ className, ...props }, ref) => (
  <DropdownMenuPrimitive.SubContent ref={ref} className={cn("z-50 min-w-[8rem] overflow-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-lg data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2", className)} {...props} />
));
DropdownMenuSubContent.displayName = DropdownMenuPrimitive.SubContent.displayName;

const DropdownMenuContent = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Content>
>(({ className, sideOffset = 4, ...props }, ref) => (
  <DropdownMenuPrimitive.Portal>
    <DropdownMenuPrimitive.Content ref={ref} sideOffset={sideOffset} className={cn("z-50 min-w-[8rem] overflow-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-md data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2", className)} {...props} />
  </DropdownMenuPrimitive.Portal>
));
DropdownMenuContent.displayName = DropdownMenuPrimitive.Content.displayName;

const DropdownMenuItem = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Item>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Item> & { inset?: boolean }
>(({ className, inset, ...props }, ref) => (
  <DropdownMenuPrimitive.Item ref={ref} className={cn("relative flex cursor-default select-none items-center gap-2 rounded-sm px-2 py-1.5 text-sm outline-none transition-colors focus:bg-accent focus:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50 [&_svg]:pointer-events-none [&_svg]:size-4 [&_svg]:shrink-0", inset && "pl-8", className)} {...props} />
));
DropdownMenuItem.displayName = DropdownMenuPrimitive.Item.displayName;

const DropdownMenuCheckboxItem = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.CheckboxItem>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.CheckboxItem>
>(({ className, children, checked, ...props }, ref) => (
  <DropdownMenuPrimitive.CheckboxItem ref={ref} className={cn("relative flex cursor-default select-none items-center rounded-sm py-1.5 pl-8 pr-2 text-sm outline-none transition-colors focus:bg-accent focus:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50", className)} checked={checked} {...props}>
    <span className="absolute left-2 flex h-3.5 w-3.5 items-center justify-center"><DropdownMenuPrimitive.ItemIndicator><Check className="h-4 w-4" /></DropdownMenuPrimitive.ItemIndicator></span>
    {children}
  </DropdownMenuPrimitive.CheckboxItem>
));
DropdownMenuCheckboxItem.displayName = DropdownMenuPrimitive.CheckboxItem.displayName;

const DropdownMenuRadioItem = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.RadioItem>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.RadioItem>
>(({ className, children, ...props }, ref) => (
  <DropdownMenuPrimitive.RadioItem ref={ref} className={cn("relative flex cursor-default select-none items-center rounded-sm py-1.5 pl-8 pr-2 text-sm outline-none transition-colors focus:bg-accent focus:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50", className)} {...props}>
    <span className="absolute left-2 flex h-3.5 w-3.5 items-center justify-center"><DropdownMenuPrimitive.ItemIndicator><Circle className="h-2 w-2 fill-current" /></DropdownMenuPrimitive.ItemIndicator></span>
    {children}
  </DropdownMenuPrimitive.RadioItem>
));
DropdownMenuRadioItem.displayName = DropdownMenuPrimitive.RadioItem.displayName;

const DropdownMenuLabel = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Label>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Label> & { inset?: boolean }
>(({ className, inset, ...props }, ref) => (
  <DropdownMenuPrimitive.Label ref={ref} className={cn("px-2 py-1.5 text-sm font-semibold", inset && "pl-8", className)} {...props} />
));
DropdownMenuLabel.displayName = DropdownMenuPrimitive.Label.displayName;

const DropdownMenuSeparator = React.forwardRef<
  React.ElementRef<typeof DropdownMenuPrimitive.Separator>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Separator>
>(({ className, ...props }, ref) => (
  <DropdownMenuPrimitive.Separator ref={ref} className={cn("-mx-1 my-1 h-px bg-muted", className)} {...props} />
));
DropdownMenuSeparator.displayName = DropdownMenuPrimitive.Separator.displayName;

const DropdownMenuShortcut = ({ className, ...props }: React.HTMLAttributes<HTMLSpanElement>) => (
  <span className={cn("ml-auto text-xs tracking-widest opacity-60", className)} {...props} />
);
DropdownMenuShortcut.displayName = "DropdownMenuShortcut";

export { DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuItem, DropdownMenuCheckboxItem, DropdownMenuRadioItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuShortcut, DropdownMenuGroup, DropdownMenuPortal, DropdownMenuSub, DropdownMenuSubContent, DropdownMenuSubTrigger, DropdownMenuRadioGroup };`,
      "src/components/ui/popover.tsx": `import * as React from "react";
import * as PopoverPrimitive from "@radix-ui/react-popover";
import { cn } from "@/lib/utils";

const Popover = PopoverPrimitive.Root;
const PopoverTrigger = PopoverPrimitive.Trigger;

const PopoverContent = React.forwardRef<
  React.ElementRef<typeof PopoverPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof PopoverPrimitive.Content>
>(({ className, align = "center", sideOffset = 4, ...props }, ref) => (
  <PopoverPrimitive.Portal>
    <PopoverPrimitive.Content ref={ref} align={align} sideOffset={sideOffset} className={cn("z-50 w-72 rounded-md border bg-popover p-4 text-popover-foreground shadow-md outline-none data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2", className)} {...props} />
  </PopoverPrimitive.Portal>
));
PopoverContent.displayName = PopoverPrimitive.Content.displayName;

export { Popover, PopoverTrigger, PopoverContent };`,
      "src/components/ui/accordion.tsx": `import * as React from "react";
import * as AccordionPrimitive from "@radix-ui/react-accordion";
import { ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";

const Accordion = AccordionPrimitive.Root;

const AccordionItem = React.forwardRef<
  React.ElementRef<typeof AccordionPrimitive.Item>,
  React.ComponentPropsWithoutRef<typeof AccordionPrimitive.Item>
>(({ className, ...props }, ref) => (
  <AccordionPrimitive.Item ref={ref} className={cn("border-b", className)} {...props} />
));
AccordionItem.displayName = "AccordionItem";

const AccordionTrigger = React.forwardRef<
  React.ElementRef<typeof AccordionPrimitive.Trigger>,
  React.ComponentPropsWithoutRef<typeof AccordionPrimitive.Trigger>
>(({ className, children, ...props }, ref) => (
  <AccordionPrimitive.Header className="flex">
    <AccordionPrimitive.Trigger ref={ref} className={cn("flex flex-1 items-center justify-between py-4 font-medium transition-all hover:underline [&[data-state=open]>svg]:rotate-180", className)} {...props}>
      {children}
      <ChevronDown className="h-4 w-4 shrink-0 transition-transform duration-200" />
    </AccordionPrimitive.Trigger>
  </AccordionPrimitive.Header>
));
AccordionTrigger.displayName = AccordionPrimitive.Trigger.displayName;

const AccordionContent = React.forwardRef<
  React.ElementRef<typeof AccordionPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof AccordionPrimitive.Content>
>(({ className, children, ...props }, ref) => (
  <AccordionPrimitive.Content ref={ref} className="overflow-hidden text-sm transition-all data-[state=closed]:animate-accordion-up data-[state=open]:animate-accordion-down" {...props}>
    <div className={cn("pb-4 pt-0", className)}>{children}</div>
  </AccordionPrimitive.Content>
));
AccordionContent.displayName = AccordionPrimitive.Content.displayName;

export { Accordion, AccordionItem, AccordionTrigger, AccordionContent };`,
      "src/components/ui/alert-dialog.tsx": `import * as React from "react";
import * as AlertDialogPrimitive from "@radix-ui/react-alert-dialog";
import { cn } from "@/lib/utils";
import { buttonVariants } from "@/components/ui/button";

const AlertDialog = AlertDialogPrimitive.Root;
const AlertDialogTrigger = AlertDialogPrimitive.Trigger;
const AlertDialogPortal = AlertDialogPrimitive.Portal;

const AlertDialogOverlay = React.forwardRef<
  React.ElementRef<typeof AlertDialogPrimitive.Overlay>,
  React.ComponentPropsWithoutRef<typeof AlertDialogPrimitive.Overlay>
>(({ className, ...props }, ref) => (
  <AlertDialogPrimitive.Overlay className={cn("fixed inset-0 z-50 bg-black/80 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0", className)} {...props} ref={ref} />
));
AlertDialogOverlay.displayName = AlertDialogPrimitive.Overlay.displayName;

const AlertDialogContent = React.forwardRef<
  React.ElementRef<typeof AlertDialogPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof AlertDialogPrimitive.Content>
>(({ className, ...props }, ref) => (
  <AlertDialogPortal>
    <AlertDialogOverlay />
    <AlertDialogPrimitive.Content ref={ref} className={cn("fixed left-[50%] top-[50%] z-50 grid w-full max-w-lg translate-x-[-50%] translate-y-[-50%] gap-4 border bg-background p-6 shadow-lg duration-200 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[state=closed]:slide-out-to-left-1/2 data-[state=closed]:slide-out-to-top-[48%] data-[state=open]:slide-in-from-left-1/2 data-[state=open]:slide-in-from-top-[48%] sm:rounded-lg", className)} {...props} />
  </AlertDialogPortal>
));
AlertDialogContent.displayName = AlertDialogPrimitive.Content.displayName;

const AlertDialogHeader = ({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
  <div className={cn("flex flex-col space-y-2 text-center sm:text-left", className)} {...props} />
);
AlertDialogHeader.displayName = "AlertDialogHeader";

const AlertDialogFooter = ({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
  <div className={cn("flex flex-col-reverse sm:flex-row sm:justify-end sm:space-x-2", className)} {...props} />
);
AlertDialogFooter.displayName = "AlertDialogFooter";

const AlertDialogTitle = React.forwardRef<
  React.ElementRef<typeof AlertDialogPrimitive.Title>,
  React.ComponentPropsWithoutRef<typeof AlertDialogPrimitive.Title>
>(({ className, ...props }, ref) => (
  <AlertDialogPrimitive.Title ref={ref} className={cn("text-lg font-semibold", className)} {...props} />
));
AlertDialogTitle.displayName = AlertDialogPrimitive.Title.displayName;

const AlertDialogDescription = React.forwardRef<
  React.ElementRef<typeof AlertDialogPrimitive.Description>,
  React.ComponentPropsWithoutRef<typeof AlertDialogPrimitive.Description>
>(({ className, ...props }, ref) => (
  <AlertDialogPrimitive.Description ref={ref} className={cn("text-sm text-muted-foreground", className)} {...props} />
));
AlertDialogDescription.displayName = AlertDialogPrimitive.Description.displayName;

const AlertDialogAction = React.forwardRef<
  React.ElementRef<typeof AlertDialogPrimitive.Action>,
  React.ComponentPropsWithoutRef<typeof AlertDialogPrimitive.Action>
>(({ className, ...props }, ref) => (
  <AlertDialogPrimitive.Action ref={ref} className={cn(buttonVariants(), className)} {...props} />
));
AlertDialogAction.displayName = AlertDialogPrimitive.Action.displayName;

const AlertDialogCancel = React.forwardRef<
  React.ElementRef<typeof AlertDialogPrimitive.Cancel>,
  React.ComponentPropsWithoutRef<typeof AlertDialogPrimitive.Cancel>
>(({ className, ...props }, ref) => (
  <AlertDialogPrimitive.Cancel ref={ref} className={cn(buttonVariants({ variant: "outline" }), "mt-2 sm:mt-0", className)} {...props} />
));
AlertDialogCancel.displayName = AlertDialogPrimitive.Cancel.displayName;

export { AlertDialog, AlertDialogPortal, AlertDialogOverlay, AlertDialogTrigger, AlertDialogContent, AlertDialogHeader, AlertDialogFooter, AlertDialogTitle, AlertDialogDescription, AlertDialogAction, AlertDialogCancel };`,
      "src/components/ui/sonner.tsx": `import { Toaster as Sonner } from "sonner";

type ToasterProps = React.ComponentProps<typeof Sonner>;

const Toaster = ({ ...props }: ToasterProps) => {
  return (
    <Sonner
      className="toaster group"
      toastOptions={{
        classNames: {
          toast: "group toast group-[.toaster]:bg-background group-[.toaster]:text-foreground group-[.toaster]:border-border group-[.toaster]:shadow-lg",
          description: "group-[.toast]:text-muted-foreground",
          actionButton: "group-[.toast]:bg-primary group-[.toast]:text-primary-foreground",
          cancelButton: "group-[.toast]:bg-muted group-[.toast]:text-muted-foreground",
        },
      }}
      {...props}
    />
  );
};

export { Toaster };`,
      "src/components/ui/radio-group.tsx": `import * as React from "react";
import * as RadioGroupPrimitive from "@radix-ui/react-radio-group";
import { Circle } from "lucide-react";
import { cn } from "@/lib/utils";

const RadioGroup = React.forwardRef<
  React.ElementRef<typeof RadioGroupPrimitive.Root>,
  React.ComponentPropsWithoutRef<typeof RadioGroupPrimitive.Root>
>(({ className, ...props }, ref) => (
  <RadioGroupPrimitive.Root className={cn("grid gap-2", className)} {...props} ref={ref} />
));
RadioGroup.displayName = RadioGroupPrimitive.Root.displayName;

const RadioGroupItem = React.forwardRef<
  React.ElementRef<typeof RadioGroupPrimitive.Item>,
  React.ComponentPropsWithoutRef<typeof RadioGroupPrimitive.Item>
>(({ className, ...props }, ref) => (
  <RadioGroupPrimitive.Item ref={ref} className={cn("aspect-square h-4 w-4 rounded-full border border-primary text-primary ring-offset-background focus:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50", className)} {...props}>
    <RadioGroupPrimitive.Indicator className="flex items-center justify-center">
      <Circle className="h-2.5 w-2.5 fill-current text-current" />
    </RadioGroupPrimitive.Indicator>
  </RadioGroupPrimitive.Item>
));
RadioGroupItem.displayName = RadioGroupPrimitive.Item.displayName;

export { RadioGroup, RadioGroupItem };`,
      "src/vite-env.d.ts": `/// <reference types="vite/client" />`,
    },
  },
  {
    id: "nextjs",
    name: "Next.js",
    description: "Next.js + TypeScript + Tailwind",
    tech_stack: "nextjs",
    dev_command: "npm run dev",
    install_command: "npm install",
    build_command: "npm run build",
    files: {
      "AI_RULES.md": `# Tech Stack
- You are building a Next.js application with the App Router.
- Use TypeScript.
- Always put source code in the src folder.
- Put pages into src/app/ using the App Router file conventions (page.tsx, layout.tsx).
- Put components into src/components/
- The main page is src/app/page.tsx
- UPDATE the main page to include the new components. OTHERWISE, the user can NOT see any components!
- Tailwind CSS: always use Tailwind CSS for styling components. Utilize Tailwind classes extensively for layout, spacing, colors, and other design aspects.

Available packages and libraries:
- The lucide-react package can be used for icons.
- Use Tailwind CSS utility classes for all styling.
`,
      "package.json": `{
  "name": "my-nextjs-app",
  "version": "0.1.0",
  "private": true,
  "scripts": {
    "dev": "next dev",
    "build": "next build",
    "start": "next start",
    "lint": "next lint"
  },
  "dependencies": {
    "next": "^15.1.0",
    "react": "^18.3.1",
    "react-dom": "^18.3.1"
  },
  "devDependencies": {
    "@types/node": "^22.10.1",
    "@types/react": "^18.3.12",
    "@types/react-dom": "^18.3.1",
    "typescript": "^5.6.3",
    "tailwindcss": "^3.4.16",
    "postcss": "^8.4.49",
    "autoprefixer": "^10.4.20"
  }
}`,
      "tsconfig.json": `{
  "compilerOptions": {
    "target": "ES2017",
    "lib": ["dom", "dom.iterable", "esnext"],
    "allowJs": true,
    "skipLibCheck": true,
    "strict": true,
    "noEmit": true,
    "esModuleInterop": true,
    "module": "esnext",
    "moduleResolution": "bundler",
    "resolveJsonModule": true,
    "isolatedModules": true,
    "jsx": "preserve",
    "incremental": true,
    "plugins": [{ "name": "next" }],
    "paths": { "@/*": ["./src/*"] }
  },
  "include": ["next-env.d.ts", "**/*.ts", "**/*.tsx", ".next/types/**/*.ts"],
  "exclude": ["node_modules"]
}`,
      "next.config.ts": `import type { NextConfig } from "next";

const nextConfig: NextConfig = {};

export default nextConfig;`,
      "tailwind.config.ts": `import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {},
  },
  plugins: [],
};
export default config;`,
      "postcss.config.mjs": `/** @type {import('postcss-load-config').Config} */
const config = {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
};

export default config;`,
      "src/app/globals.css": `@tailwind base;
@tailwind components;
@tailwind utilities;

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}`,
      "src/app/layout.tsx": `import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Next.js App",
  description: "Created with Next.js",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}`,
      "src/app/page.tsx": `export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-center p-24">
      <h1 className="text-4xl font-bold">Welcome to Next.js</h1>
      <p className="mt-4 text-lg text-gray-600">
        Edit <code className="bg-gray-100 px-2 py-1 rounded">src/app/page.tsx</code> to get started.
      </p>
    </main>
  );
}`,
    },
  },
  {
    id: "vue-vite",
    name: "Vue (Vite)",
    description: "Vue 3 + Vite + TypeScript",
    tech_stack: "vue",
    dev_command: "npm run dev",
    install_command: "npm install",
    build_command: "npm run build",
    files: {
      "AI_RULES.md": `# Tech Stack
- You are building a Vue 3 application with Vite.
- Use TypeScript with \`<script setup lang="ts">\` syntax.
- Always put source code in the src folder.
- Put components into src/components/
- The main component is src/App.vue
- UPDATE App.vue to import and include new components. OTHERWISE, the user can NOT see any components!
- Use scoped styles or a CSS framework for styling.

Available packages and libraries:
- Vue 3 Composition API is available.
`,
      "package.json": `{
  "name": "my-vue-app",
  "private": true,
  "version": "0.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vue-tsc -b && vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "vue": "^3.5.13"
  },
  "devDependencies": {
    "@vitejs/plugin-vue": "^5.2.1",
    "typescript": "~5.6.2",
    "vite": "^6.0.1",
    "vue-tsc": "^2.1.10"
  }
}`,
      "vite.config.ts": `import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
})`,
      "tsconfig.json": `{
  "files": [],
  "references": [
    { "path": "./tsconfig.app.json" },
    { "path": "./tsconfig.node.json" }
  ]
}`,
      "tsconfig.app.json": `{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "module": "ESNext",
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "isolatedModules": true,
    "moduleDetection": "force",
    "noEmit": true,
    "jsx": "preserve",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true
  },
  "include": ["src/**/*.ts", "src/**/*.tsx", "src/**/*.vue"]
}`,
      "tsconfig.node.json": `{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2023"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "isolatedModules": true,
    "moduleDetection": "force",
    "noEmit": true,
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true
  },
  "include": ["vite.config.ts"]
}`,
      "index.html": `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Vue App</title>
  </head>
  <body>
    <div id="app"></div>
    <script type="module" src="/src/main.ts"></script>
  </body>
</html>`,
      "src/main.ts": `import { createApp } from 'vue'
import './style.css'
import App from './App.vue'

createApp(App).mount('#app')`,
      "src/App.vue": `<script setup lang="ts">
import { ref } from 'vue'

const count = ref(0)
</script>

<template>
  <div class="app">
    <h1>Vue 3 + Vite</h1>
    <div class="card">
      <button type="button" @click="count++">count is {{ count }}</button>
      <p>Edit <code>src/App.vue</code> and save to test HMR</p>
    </div>
  </div>
</template>

<style scoped>
.app {
  max-width: 1280px;
  margin: 0 auto;
  padding: 2rem;
  text-align: center;
}
.card {
  padding: 2em;
}
button {
  border-radius: 8px;
  border: 1px solid transparent;
  padding: 0.6em 1.2em;
  font-size: 1em;
  font-weight: 500;
  font-family: inherit;
  background-color: #1a1a1a;
  color: #fff;
  cursor: pointer;
  transition: border-color 0.25s;
}
button:hover {
  border-color: #646cff;
}
</style>`,
      "src/style.css": `body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  -webkit-font-smoothing: antialiased;
  min-height: 100vh;
}`,
      "src/vite-env.d.ts": `/// <reference types="vite/client" />`,
    },
  },
  {
    id: "d2e-researcher-plugin",
    name: "D2E Researcher Plugin",
    description: "Full-stack single-spa researcher portal plugin with Deno backend",
    tech_stack: "d2e-react",
    dev_command: "npm run dev",
    install_command: "npm install",
    build_command: "npm run build",
    files: {
      "AI_RULES.md": `# Tech Stack
- You are building a D2E portal plugin (micro-frontend).
- Tech stack: React 18 + MUI 5 + single-spa + Vite (SystemJS output) frontend; Deno edge functions backend.
- D2E theme: primary #000080 (navy), background #f2f0f1, table header #ebf1f8.
- Use TypeScript throughout.
- Production entry: src/lifecycles.tsx (single-spa lifecycle exports, SystemJS format).
- Dev entry: src/main.tsx (standalone portal mock for devx preview).
- Use @emotion/styled and MUI sx prop for styling. Do NOT use Tailwind CSS.
- Put components in src/components/
- Put pages in src/pages/
- Put backend Deno functions in functions/
- Portal props available via PortalContext: getToken, username, datasetId, studyId, features, locale, containerId, appId, apiBase.
- Always use PortalContext to access portal APIs (e.g. getToken() for auth headers).
- Use apiBase from PortalContext as the base URL for all backend API calls (e.g. fetch(\`\${apiBase}/items\`)).
- NEVER change apiBase in main.tsx — it must stay as '/plugins/trex/__APP_ID__/api'. The trex server routes requests to the backend functions.
- NEVER change getToken in main.tsx — the preview passes the auth token via URL query parameter.
- UPDATE src/pages/HomePage.tsx or add new pages. The App.tsx routes to pages.

## D2E Portal Styling Guidelines
- Font sizes: h4 page titles 1.5rem (24px) bold, h5 1.25rem (20px) semibold, h6 section headers 1.125rem (18px) semibold, body/tables/buttons 0.875rem (14px), small text 12px
- Border radius: 8px for buttons, 16px for cards, 32px for dialogs
- Buttons: disableElevation, textTransform "none", fontSize 0.875rem (14px), outlined variant uses 2px border
- Cards: borderRadius 16px, boxShadow "0 3px 12px 0 #dedcda", border "1px solid #dedcda"
- Card header: padding 20px 24px, border-bottom 1px solid #dedcda, title 18px weight 500
- Tables: header background #ebf1f8, header text #000080 weight 500 14px, body text #555555 14px
- Dialogs: borderRadius 32px on paper, title 18px semibold
- Colors: primary #000080, text.secondary #555555, divider #dedcda, background #f2f0f1
- Spacing: use multiples of 8px (8, 16, 24, 32)
- Shadows: cards use "0 3px 12px 0 #dedcda"
- Tabs: textTransform "none", indicator height 4px
- All MUI buttons must set disableElevation
`,
      "package.json": `{
  "name": "@trex/__APP_ID__",
  "private": true,
  "version": "0.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "@emotion/react": "^11.11.1",
    "@emotion/styled": "^11.11.0",
    "@mui/icons-material": "^5.8.3",
    "@mui/material": "^5.8.3",
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "single-spa-react": "^6.0.2"
  },
  "devDependencies": {
    "@types/react": "^18.2.0",
    "@types/react-dom": "^18.2.0",
    "@vitejs/plugin-react": "^4.3.4",
    "typescript": "~5.6.2",
    "vite": "^6.0.1",
    "vite-plugin-css-injected-by-js": "^3.5.2"
  },
  "trex": {
    "functions": {
      "api": [
        {
          "source": "/__APP_ID__/api",
          "function": "/functions"
        }
      ],
      "roles": {
        "__APP_ID__-user": ["__APP_ID__:read", "__APP_ID__:write"]
      },
      "scopes": [
        { "path": "/plugins/trex/__APP_ID__/api/.*", "scopes": ["__APP_ID__:read"] }
      ]
    },
    "ui": {
      "routes": [
        {
          "path": "/app",
          "dir": "dist",
          "spa": true
        }
      ]
    }
  }
}`,
      "vite.config.ts": `import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import cssInjectedByJsPlugin from 'vite-plugin-css-injected-by-js';

export default defineConfig({
  plugins: [react(), cssInjectedByJsPlugin()],
  build: {
    lib: {
      entry: 'src/lifecycles.tsx',
      formats: ['system'],
      fileName: 'lifecycles',
    },
    rollupOptions: {
      external: ['react', 'react-dom'],
    },
  },
});`,
      "tsconfig.json": `{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true
  },
  "include": ["src"]
}`,
      "index.html": `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>D2E Researcher Plugin</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>`,
      "src/main.tsx": `import { createRoot } from 'react-dom/client';
import App from './App';
import type { PortalProps } from './types/portal';

/**
 * Portal mock harness for standalone dev preview.
 * Simulates the D2E portal shell so the plugin renders in the devx iframe.
 */
const mockPortalProps: PortalProps = {
  appId: '__APP_ID__',
  containerId: 'root',
  getToken: async () => new URLSearchParams(window.location.search).get('token') || import.meta.env.VITE_MOCK_TOKEN || 'mock-jwt-token-for-dev',
  username: 'researcher',
  userId: 'user-1',
  datasetId: 'dataset-1',
  studyId: 'study-1',
  features: [],
  locale: 'en',
  apiBase: '/plugins/trex/__APP_ID__/api',
  autoMount: true,
};

// Simulate portal prop change events (for testing prop update handling)
window.addEventListener('DOMContentLoaded', () => {
  setTimeout(() => {
    window.dispatchEvent(new CustomEvent('custom-props-changed', { detail: mockPortalProps }));
  }, 100);
});

const root = createRoot(document.getElementById('root')!);
root.render(<App {...mockPortalProps} />);
`,
      "src/lifecycles.tsx": `import React from 'react';
import ReactDOM from 'react-dom';
import singleSpaReact from 'single-spa-react';
import App from './App';

const lifecycles = singleSpaReact({
  React,
  ReactDOM,
  rootComponent: App,
  domElementGetter: (props: any) => {
    return document.getElementById(props.containerId) || document.createElement('div');
  },
  errorBoundary() {
    return <div>Plugin failed to load.</div>;
  },
});

export const { bootstrap, mount, unmount } = lifecycles;
`,
      "src/components/PortalShell.tsx": `import { AppBar, Toolbar, Typography, Button, Chip, Box } from '@mui/material';
import { usePortal } from '../context/PortalContext';

/**
 * Fake portal header shown only in dev preview.
 * Simulates the D2E portal navigation bar so the plugin
 * looks closer to its production environment during development.
 */
export default function PortalShell() {
  const { username, appId } = usePortal();
  return (
    <AppBar position="static" sx={{ bgcolor: '#000080', boxShadow: '0 2px 8px rgba(0,0,0,0.15)' }}>
      <Toolbar variant="dense" sx={{ minHeight: 48, gap: 1 }}>
        <Typography sx={{ fontWeight: 300, letterSpacing: '0.15em', fontSize: '1.1rem', color: '#fff' }}>
          D2E
        </Typography>
        <Chip label="DEV PREVIEW" size="small" sx={{ bgcolor: 'rgba(255,255,255,0.15)', color: '#fff', fontSize: '0.65rem', height: 20 }} />
        <Box sx={{ flex: 1 }} />
        {['Studies', 'Datasets', 'Plugins'].map((item) => (
          <Button key={item} size="small" sx={{ color: 'rgba(255,255,255,0.7)', textTransform: 'none', fontSize: '0.8rem', minWidth: 'auto' }}>
            {item}
          </Button>
        ))}
        <Box sx={{ flex: 1 }} />
        <Chip label={username} size="small" variant="outlined" sx={{ color: '#fff', borderColor: 'rgba(255,255,255,0.3)', fontSize: '0.75rem' }} />
      </Toolbar>
    </AppBar>
  );
}
`,
      "src/App.tsx": `import { ThemeProvider, CssBaseline } from '@mui/material';
import { theme } from './theme';
import { PortalProvider } from './context/PortalContext';
import PortalShell from './components/PortalShell';
import HomePage from './pages/HomePage';
import type { PortalProps } from './types/portal';

export default function App(props: PortalProps) {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <PortalProvider value={props}>
        {import.meta.env.DEV && <PortalShell />}
        <HomePage />
      </PortalProvider>
    </ThemeProvider>
  );
}
`,
      "src/theme.ts": `import { createTheme } from '@mui/material/styles';

export const theme = createTheme({
  shape: { borderRadius: 8 },
  palette: {
    primary: { main: '#000080' },
    background: { default: '#f2f0f1', paper: '#ffffff' },
    text: { primary: '#000080', secondary: '#555555' },
    divider: '#dedcda',
  },
  typography: {
    fontFamily: '"GT-America", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, "Fira Sans", "Droid Sans", "Helvetica Neue", sans-serif',
    h4: { fontSize: '1.5rem', fontWeight: 700 },
    h5: { fontSize: '1.25rem', fontWeight: 600 },
    h6: { fontSize: '1.125rem', fontWeight: 600 },
    body1: { fontSize: '0.875rem' },
    body2: { fontSize: '0.875rem' },
  },
  components: {
    MuiButton: {
      defaultProps: { disableElevation: true },
      styleOverrides: {
        root: { borderRadius: 8, textTransform: 'none', fontSize: '0.875rem' },
        outlined: { borderWidth: 2 },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: { borderRadius: 16, boxShadow: '0 3px 12px 0 #dedcda', border: '1px solid #dedcda' },
      },
    },
    MuiDialog: {
      styleOverrides: {
        paper: { borderRadius: 32 },
      },
    },
    MuiTableHead: {
      styleOverrides: {
        root: { backgroundColor: '#ebf1f8' },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        head: { fontWeight: 500, color: '#000080', fontSize: '14px' },
        body: { fontSize: '14px', color: '#555555' },
      },
    },
    MuiTab: {
      styleOverrides: {
        root: { textTransform: 'none' },
      },
    },
  },
});
`,
      "src/types/portal.ts": `export interface PortalProps {
  appId: string;
  containerId: string;
  getToken: () => Promise<string>;
  username: string;
  userId: string;
  datasetId: string;
  studyId: string;
  features: string[];
  locale: string;
  apiBase: string;
  autoMount?: boolean;
}
`,
      "src/context/PortalContext.tsx": `import { createContext, useContext } from 'react';
import type { PortalProps } from '../types/portal';

const PortalContext = createContext<PortalProps | null>(null);

export function PortalProvider({ value, children }: { value: PortalProps; children: React.ReactNode }) {
  return <PortalContext.Provider value={value}>{children}</PortalContext.Provider>;
}

export function usePortal(): PortalProps {
  const ctx = useContext(PortalContext);
  if (!ctx) throw new Error('usePortal must be used within a PortalProvider');
  return ctx;
}
`,
      "src/pages/HomePage.tsx": `import { useState, useEffect, useCallback } from 'react';
import { Container, Typography, Card, CardContent, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper, Box, Button, CircularProgress, TextField, Dialog, DialogTitle, DialogContent, DialogActions } from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import AddIcon from '@mui/icons-material/Add';
import { usePortal } from '../context/PortalContext';

interface Item {
  id: string;
  name: string;
  description: string;
  createdAt: string;
}

export default function HomePage() {
  const { username, datasetId, getToken, apiBase } = usePortal();
  const [items, setItems] = useState<Item[]>([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [newName, setNewName] = useState('');
  const [newDesc, setNewDesc] = useState('');

  const fetchItems = useCallback(async () => {
    setLoading(true);
    try {
      const token = await getToken();
      const res = await fetch(\`\${apiBase}/items\`, {
        headers: { Authorization: \`Bearer \${token}\` },
      });
      const json = await res.json();
      setItems(json.data || []);
    } catch (err) {
      console.error('Failed to fetch items:', err);
    } finally {
      setLoading(false);
    }
  }, [getToken, apiBase]);

  useEffect(() => { fetchItems(); }, [fetchItems]);

  const handleAdd = async () => {
    try {
      const token = await getToken();
      await fetch(\`\${apiBase}/items\`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: \`Bearer \${token}\` },
        body: JSON.stringify({ name: newName, description: newDesc }),
      });
      setDialogOpen(false);
      setNewName('');
      setNewDesc('');
      fetchItems();
    } catch (err) {
      console.error('Failed to create item:', err);
    }
  };

  return (
    <Container maxWidth="md" sx={{ py: 4 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h4" gutterBottom>
            Researcher Plugin
          </Typography>
          <Typography variant="body1" color="text.secondary">
            Welcome, {username}. Dataset: {datasetId}
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1 }}>
          <Button variant="outlined" startIcon={<RefreshIcon />} onClick={fetchItems}>
            Refresh
          </Button>
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setDialogOpen(true)}>
            Add Item
          </Button>
        </Box>
      </Box>

      <Card sx={{ mb: 3 }}>
        <Box sx={{ px: 3, py: 2.5, borderBottom: '1px solid #dedcda' }}>
          <Typography sx={{ fontSize: '18px', fontWeight: 500 }}>
            Dataset Overview
          </Typography>
        </Box>
        <CardContent>
          {loading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
              <CircularProgress />
            </Box>
          ) : (
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Name</TableCell>
                    <TableCell>Description</TableCell>
                    <TableCell>Created</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {items.map((row) => (
                    <TableRow key={row.id}>
                      <TableCell>{row.name}</TableCell>
                      <TableCell>{row.description}</TableCell>
                      <TableCell>{new Date(row.createdAt).toLocaleDateString()}</TableCell>
                    </TableRow>
                  ))}
                  {items.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={3} align="center" sx={{ py: 3 }}>
                        No items yet. Click "Add Item" to create one.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </CardContent>
      </Card>

      <Box sx={{ p: 2, bgcolor: 'background.paper', borderRadius: 2, border: '1px solid', borderColor: 'divider' }}>
        <Typography variant="body2" color="text.secondary">
          This is a starter template. Edit <code>src/pages/HomePage.tsx</code> to build your researcher plugin.
        </Typography>
      </Box>

      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Add New Item</DialogTitle>
        <DialogContent sx={{ display: 'flex', flexDirection: 'column', gap: 2, pt: '16px !important' }}>
          <TextField label="Name" value={newName} onChange={(e) => setNewName(e.target.value)} fullWidth />
          <TextField label="Description" value={newDesc} onChange={(e) => setNewDesc(e.target.value)} fullWidth multiline rows={3} />
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={handleAdd} disabled={!newName.trim()}>Create</Button>
        </DialogActions>
      </Dialog>
    </Container>
  );
}
`,
      "functions/deno.json": `{
  "compilerOptions": {
    "strict": true,
    "noImplicitAny": true
  }
}`,
      "functions/index.ts": `const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

import { getItems, createItem } from './routes/items.ts';

Deno.serve({ port: 8000 }, async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const url = new URL(req.url);
  // Strip plugin prefix — the full path includes /plugins/trex/{appId}/api/...
  // We only need the /api/... suffix for routing.
  const fullPath = url.pathname;
  const apiIdx = fullPath.indexOf('/api');
  const path = apiIdx >= 0 ? fullPath.slice(apiIdx) : fullPath;

  try {
    if (path === '/api/health' && req.method === 'GET') {
      return Response.json({ status: 'ok', timestamp: new Date().toISOString() }, { headers: corsHeaders });
    }
    if (path === '/api/items' && req.method === 'GET') {
      return Response.json(await getItems(), { headers: corsHeaders });
    }
    if (path === '/api/items' && req.method === 'POST') {
      const body = await req.json();
      return Response.json(await createItem(body), { status: 201, headers: corsHeaders });
    }

    return Response.json({ error: 'Not found' }, { status: 404, headers: corsHeaders });
  } catch (err) {
    return Response.json({ error: (err as Error).message }, { status: 500, headers: corsHeaders });
  }
});
`,
      "functions/routes/items.ts": `import type { Item, ApiResponse } from '../types.ts';

const items: Item[] = [
  { id: '1', name: 'Sample Item 1', description: 'First sample item', createdAt: new Date().toISOString() },
  { id: '2', name: 'Sample Item 2', description: 'Second sample item', createdAt: new Date().toISOString() },
];

export async function getItems(): Promise<ApiResponse<Item[]>> {
  return { data: items, total: items.length };
}

export async function createItem(input: Partial<Item>): Promise<ApiResponse<Item>> {
  const item: Item = {
    id: crypto.randomUUID(),
    name: input.name || 'Untitled',
    description: input.description || '',
    createdAt: new Date().toISOString(),
  };
  items.push(item);
  return { data: item, total: items.length };
}
`,
      "functions/types.ts": `export interface Item {
  id: string;
  name: string;
  description: string;
  createdAt: string;
}

export interface ApiResponse<T> {
  data: T;
  total: number;
}

export interface ErrorResponse {
  error: string;
  details?: string;
}
`,
    },
  },
  {
    id: "d2e-admin-plugin",
    name: "D2E Admin Plugin",
    description: "Full-stack single-spa admin portal plugin with Deno backend",
    tech_stack: "d2e-react",
    dev_command: "npm run dev",
    install_command: "npm install",
    build_command: "npm run build",
    files: {
      "AI_RULES.md": `# Tech Stack
- You are building a D2E admin portal plugin (micro-frontend).
- Tech stack: React 18 + MUI 5 + single-spa + Vite (SystemJS output) frontend; Deno edge functions backend.
- D2E theme: primary #000080 (navy), background #f2f0f1, table header #ebf1f8.
- Use TypeScript throughout.
- Production entry: src/lifecycles.tsx (single-spa lifecycle exports, SystemJS format).
- Dev entry: src/main.tsx (standalone portal mock for devx preview).
- Use @emotion/styled and MUI sx prop for styling. Do NOT use Tailwind CSS.
- Put components in src/components/
- Put pages in src/pages/
- Put backend Deno functions in functions/
- Portal props available via PortalContext: getToken, username, system, userId, data, apiBase.
- This is an admin plugin: manages users, configuration, and infrastructure. Required role: SYSTEM_ADMIN.
- Always use PortalContext to access portal APIs (e.g. getToken() for auth headers).
- Use apiBase from PortalContext as the base URL for all backend API calls (e.g. fetch(\`\${apiBase}/users\`)).
- NEVER change apiBase in main.tsx — it must stay as '/plugins/trex/__APP_ID__/api'. The trex server routes requests to the backend functions.
- NEVER change getToken in main.tsx — the preview passes the auth token via URL query parameter.
- UPDATE src/pages/HomePage.tsx or add new pages. The App.tsx routes to pages.

## D2E Portal Styling Guidelines
- Font sizes: h4 page titles 1.5rem (24px) bold, h5 1.25rem (20px) semibold, h6 section headers 1.125rem (18px) semibold, body/tables/buttons 0.875rem (14px), small text 12px
- Border radius: 8px for buttons, 16px for cards, 32px for dialogs
- Buttons: disableElevation, textTransform "none", fontSize 0.875rem (14px), outlined variant uses 2px border
- Cards: borderRadius 16px, boxShadow "0 3px 12px 0 #dedcda", border "1px solid #dedcda"
- Card header: padding 20px 24px, border-bottom 1px solid #dedcda, title 18px weight 500
- Tables: header background #ebf1f8, header text #000080 weight 500 14px, body text #555555 14px
- Dialogs: borderRadius 32px on paper, title 18px semibold
- Colors: primary #000080, text.secondary #555555, divider #dedcda, background #f2f0f1
- Spacing: use multiples of 8px (8, 16, 24, 32)
- Shadows: cards use "0 3px 12px 0 #dedcda"
- Tabs: textTransform "none", indicator height 4px
- All MUI buttons must set disableElevation
`,
      "package.json": `{
  "name": "@trex/__APP_ID__",
  "private": true,
  "version": "0.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "@emotion/react": "^11.11.1",
    "@emotion/styled": "^11.11.0",
    "@mui/icons-material": "^5.8.3",
    "@mui/material": "^5.8.3",
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "single-spa-react": "^6.0.2"
  },
  "devDependencies": {
    "@types/react": "^18.2.0",
    "@types/react-dom": "^18.2.0",
    "@vitejs/plugin-react": "^4.3.4",
    "typescript": "~5.6.2",
    "vite": "^6.0.1",
    "vite-plugin-css-injected-by-js": "^3.5.2"
  },
  "trex": {
    "functions": {
      "api": [
        {
          "source": "/__APP_ID__/api",
          "function": "/functions"
        }
      ],
      "roles": {
        "__APP_ID__-admin": ["__APP_ID__:read", "__APP_ID__:write"]
      },
      "scopes": [
        { "path": "/plugins/trex/__APP_ID__/api/.*", "scopes": ["__APP_ID__:read"] }
      ]
    },
    "ui": {
      "routes": [
        {
          "path": "/app",
          "dir": "dist",
          "spa": true
        }
      ]
    }
  }
}`,
      "vite.config.ts": `import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import cssInjectedByJsPlugin from 'vite-plugin-css-injected-by-js';

export default defineConfig({
  plugins: [react(), cssInjectedByJsPlugin()],
  build: {
    lib: {
      entry: 'src/lifecycles.tsx',
      formats: ['system'],
      fileName: 'lifecycles',
    },
    rollupOptions: {
      external: ['react', 'react-dom'],
    },
  },
});`,
      "tsconfig.json": `{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true
  },
  "include": ["src"]
}`,
      "index.html": `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>D2E Admin Plugin</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>`,
      "src/main.tsx": `import { createRoot } from 'react-dom/client';
import App from './App';
import type { AdminPortalProps } from './types/portal';

/**
 * Portal mock harness for standalone dev preview.
 * Simulates the D2E portal shell so the plugin renders in the devx iframe.
 */
const mockPortalProps: AdminPortalProps = {
  appId: '__APP_ID__',
  containerId: 'root',
  getToken: async () => new URLSearchParams(window.location.search).get('token') || import.meta.env.VITE_MOCK_TOKEN || 'mock-jwt-token-for-dev',
  username: 'admin',
  userId: 'admin-1',
  system: 'default',
  data: {},
  features: [],
  locale: 'en',
  apiBase: '/plugins/trex/__APP_ID__/api',
  autoMount: true,
};

// Simulate portal prop change events (for testing prop update handling)
window.addEventListener('DOMContentLoaded', () => {
  setTimeout(() => {
    window.dispatchEvent(new CustomEvent('custom-props-changed', { detail: mockPortalProps }));
  }, 100);
});

const root = createRoot(document.getElementById('root')!);
root.render(<App {...mockPortalProps} />);
`,
      "src/lifecycles.tsx": `import React from 'react';
import ReactDOM from 'react-dom';
import singleSpaReact from 'single-spa-react';
import App from './App';

const lifecycles = singleSpaReact({
  React,
  ReactDOM,
  rootComponent: App,
  domElementGetter: (props: any) => {
    return document.getElementById(props.containerId) || document.createElement('div');
  },
  errorBoundary() {
    return <div>Plugin failed to load.</div>;
  },
});

export const { bootstrap, mount, unmount } = lifecycles;
`,
      "src/components/PortalShell.tsx": `import { AppBar, Toolbar, Typography, Button, Chip, Box } from '@mui/material';
import { usePortal } from '../context/PortalContext';

/**
 * Fake portal header shown only in dev preview.
 * Simulates the D2E admin portal navigation bar so the plugin
 * looks closer to its production environment during development.
 */
export default function PortalShell() {
  const { username } = usePortal();
  return (
    <AppBar position="static" sx={{ bgcolor: '#000080', boxShadow: '0 2px 8px rgba(0,0,0,0.15)' }}>
      <Toolbar variant="dense" sx={{ minHeight: 48, gap: 1 }}>
        <Typography sx={{ fontWeight: 300, letterSpacing: '0.15em', fontSize: '1.1rem', color: '#fff' }}>
          D2E
        </Typography>
        <Chip label="ADMIN" size="small" sx={{ bgcolor: 'rgba(255,255,255,0.15)', color: '#fff', fontSize: '0.65rem', height: 20 }} />
        <Chip label="DEV PREVIEW" size="small" sx={{ bgcolor: 'rgba(255,255,255,0.1)', color: 'rgba(255,255,255,0.6)', fontSize: '0.6rem', height: 18 }} />
        <Box sx={{ flex: 1 }} />
        {['Users', 'Settings', 'Monitoring'].map((item) => (
          <Button key={item} size="small" sx={{ color: 'rgba(255,255,255,0.7)', textTransform: 'none', fontSize: '0.8rem', minWidth: 'auto' }}>
            {item}
          </Button>
        ))}
        <Box sx={{ flex: 1 }} />
        <Chip label={username} size="small" variant="outlined" sx={{ color: '#fff', borderColor: 'rgba(255,255,255,0.3)', fontSize: '0.75rem' }} />
      </Toolbar>
    </AppBar>
  );
}
`,
      "src/App.tsx": `import { ThemeProvider, CssBaseline } from '@mui/material';
import { theme } from './theme';
import { PortalProvider } from './context/PortalContext';
import PortalShell from './components/PortalShell';
import HomePage from './pages/HomePage';
import type { AdminPortalProps } from './types/portal';

export default function App(props: AdminPortalProps) {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <PortalProvider value={props}>
        {import.meta.env.DEV && <PortalShell />}
        <HomePage />
      </PortalProvider>
    </ThemeProvider>
  );
}
`,
      "src/theme.ts": `import { createTheme } from '@mui/material/styles';

export const theme = createTheme({
  shape: { borderRadius: 8 },
  palette: {
    primary: { main: '#000080' },
    background: { default: '#f2f0f1', paper: '#ffffff' },
    text: { primary: '#000080', secondary: '#555555' },
    divider: '#dedcda',
  },
  typography: {
    fontFamily: '"GT-America", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, "Fira Sans", "Droid Sans", "Helvetica Neue", sans-serif',
    h4: { fontSize: '1.5rem', fontWeight: 700 },
    h5: { fontSize: '1.25rem', fontWeight: 600 },
    h6: { fontSize: '1.125rem', fontWeight: 600 },
    body1: { fontSize: '0.875rem' },
    body2: { fontSize: '0.875rem' },
  },
  components: {
    MuiButton: {
      defaultProps: { disableElevation: true },
      styleOverrides: {
        root: { borderRadius: 8, textTransform: 'none', fontSize: '0.875rem' },
        outlined: { borderWidth: 2 },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: { borderRadius: 16, boxShadow: '0 3px 12px 0 #dedcda', border: '1px solid #dedcda' },
      },
    },
    MuiDialog: {
      styleOverrides: {
        paper: { borderRadius: 32 },
      },
    },
    MuiTableHead: {
      styleOverrides: {
        root: { backgroundColor: '#ebf1f8' },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        head: { fontWeight: 500, color: '#000080', fontSize: '14px' },
        body: { fontSize: '14px', color: '#555555' },
      },
    },
    MuiTab: {
      styleOverrides: {
        root: { textTransform: 'none' },
      },
    },
  },
});
`,
      "src/types/portal.ts": `export interface AdminPortalProps {
  appId: string;
  containerId: string;
  getToken: () => Promise<string>;
  username: string;
  userId: string;
  system: string;
  data: Record<string, unknown>;
  features: string[];
  locale: string;
  apiBase: string;
  autoMount?: boolean;
}
`,
      "src/context/PortalContext.tsx": `import { createContext, useContext } from 'react';
import type { AdminPortalProps } from '../types/portal';

const PortalContext = createContext<AdminPortalProps | null>(null);

export function PortalProvider({ value, children }: { value: AdminPortalProps; children: React.ReactNode }) {
  return <PortalContext.Provider value={value}>{children}</PortalContext.Provider>;
}

export function usePortal(): AdminPortalProps {
  const ctx = useContext(PortalContext);
  if (!ctx) throw new Error('usePortal must be used within a PortalProvider');
  return ctx;
}
`,
      "src/pages/HomePage.tsx": `import { useState, useEffect, useCallback } from 'react';
import { Container, Typography, Card, CardContent, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Paper, Box, Chip, Button, CircularProgress, TextField, Dialog, DialogTitle, DialogContent, DialogActions } from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';
import PersonAddIcon from '@mui/icons-material/PersonAdd';
import { usePortal } from '../context/PortalContext';

interface User {
  id: string;
  name: string;
  email: string;
  role: string;
  status: string;
  createdAt: string;
}

export default function HomePage() {
  const { username, system, getToken, apiBase } = usePortal();
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [newName, setNewName] = useState('');
  const [newEmail, setNewEmail] = useState('');

  const fetchUsers = useCallback(async () => {
    setLoading(true);
    try {
      const token = await getToken();
      const res = await fetch(\`\${apiBase}/users\`, {
        headers: { Authorization: \`Bearer \${token}\` },
      });
      const json = await res.json();
      setUsers(json.data || []);
    } catch (err) {
      console.error('Failed to fetch users:', err);
    } finally {
      setLoading(false);
    }
  }, [getToken, apiBase]);

  useEffect(() => { fetchUsers(); }, [fetchUsers]);

  const handleAdd = async () => {
    try {
      const token = await getToken();
      await fetch(\`\${apiBase}/users\`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: \`Bearer \${token}\` },
        body: JSON.stringify({ name: newName, email: newEmail }),
      });
      setDialogOpen(false);
      setNewName('');
      setNewEmail('');
      fetchUsers();
    } catch (err) {
      console.error('Failed to create user:', err);
    }
  };

  return (
    <Container maxWidth="md" sx={{ py: 4 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h4" gutterBottom>
            Admin Plugin
          </Typography>
          <Typography variant="body1" color="text.secondary">
            Welcome, {username}. System: {system}
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', gap: 1 }}>
          <Button variant="outlined" startIcon={<RefreshIcon />} onClick={fetchUsers}>
            Refresh
          </Button>
          <Button variant="contained" startIcon={<PersonAddIcon />} onClick={() => setDialogOpen(true)}>
            Add User
          </Button>
        </Box>
      </Box>

      <Card sx={{ mb: 3 }}>
        <Box sx={{ px: 3, py: 2.5, borderBottom: '1px solid #dedcda' }}>
          <Typography sx={{ fontSize: '18px', fontWeight: 500 }}>
            User Management
          </Typography>
        </Box>
        <CardContent>
          {loading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
              <CircularProgress />
            </Box>
          ) : (
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Name</TableCell>
                    <TableCell>Email</TableCell>
                    <TableCell>Role</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {users.map((user) => (
                    <TableRow key={user.id}>
                      <TableCell>{user.name}</TableCell>
                      <TableCell>{user.email}</TableCell>
                      <TableCell>{user.role}</TableCell>
                      <TableCell>
                        <Chip
                          label={user.status}
                          size="small"
                          color={user.status === 'active' ? 'success' : 'default'}
                          variant="outlined"
                        />
                      </TableCell>
                    </TableRow>
                  ))}
                  {users.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={4} align="center" sx={{ py: 3 }}>
                        No users yet. Click "Add User" to create one.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </CardContent>
      </Card>

      <Card>
        <Box sx={{ px: 3, py: 2.5, borderBottom: '1px solid #dedcda' }}>
          <Typography sx={{ fontSize: '18px', fontWeight: 500 }}>
            System Info
          </Typography>
        </Box>
        <CardContent>
          <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 1 }}>
            <Typography variant="body2" color="text.secondary">System:</Typography>
            <Typography variant="body2">{system}</Typography>
            <Typography variant="body2" color="text.secondary">Admin:</Typography>
            <Typography variant="body2">{username}</Typography>
            <Typography variant="body2" color="text.secondary">Total Users:</Typography>
            <Typography variant="body2">{users.length}</Typography>
          </Box>
        </CardContent>
      </Card>

      <Box sx={{ mt: 3, p: 2, bgcolor: 'background.paper', borderRadius: 2, border: '1px solid', borderColor: 'divider' }}>
        <Typography variant="body2" color="text.secondary">
          This is a starter template. Edit <code>src/pages/HomePage.tsx</code> to build your admin plugin.
        </Typography>
      </Box>

      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Add New User</DialogTitle>
        <DialogContent sx={{ display: 'flex', flexDirection: 'column', gap: 2, pt: '16px !important' }}>
          <TextField label="Name" value={newName} onChange={(e) => setNewName(e.target.value)} fullWidth />
          <TextField label="Email" value={newEmail} onChange={(e) => setNewEmail(e.target.value)} fullWidth type="email" />
        </DialogContent>
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={handleAdd} disabled={!newName.trim() || !newEmail.trim()}>Create</Button>
        </DialogActions>
      </Dialog>
    </Container>
  );
}
`,
      "functions/deno.json": `{
  "compilerOptions": {
    "strict": true,
    "noImplicitAny": true
  }
}`,
      "functions/index.ts": `const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

import { getUsers, createUser } from './routes/users.ts';

Deno.serve({ port: 8000 }, async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  const url = new URL(req.url);
  // Strip plugin prefix — the full path includes /plugins/trex/{appId}/api/...
  // We only need the /api/... suffix for routing.
  const fullPath = url.pathname;
  const apiIdx = fullPath.indexOf('/api');
  const path = apiIdx >= 0 ? fullPath.slice(apiIdx) : fullPath;

  try {
    if (path === '/api/health' && req.method === 'GET') {
      return Response.json({ status: 'ok', timestamp: new Date().toISOString() }, { headers: corsHeaders });
    }
    if (path === '/api/users' && req.method === 'GET') {
      return Response.json(await getUsers(), { headers: corsHeaders });
    }
    if (path === '/api/users' && req.method === 'POST') {
      const body = await req.json();
      return Response.json(await createUser(body), { status: 201, headers: corsHeaders });
    }

    return Response.json({ error: 'Not found' }, { status: 404, headers: corsHeaders });
  } catch (err) {
    return Response.json({ error: (err as Error).message }, { status: 500, headers: corsHeaders });
  }
});
`,
      "functions/routes/users.ts": `import type { User, ApiResponse } from '../types.ts';

const users: User[] = [
  { id: '1', name: 'Alice Johnson', email: 'alice@example.com', role: 'SYSTEM_ADMIN', status: 'active', createdAt: new Date().toISOString() },
  { id: '2', name: 'Bob Smith', email: 'bob@example.com', role: 'RESEARCHER', status: 'active', createdAt: new Date().toISOString() },
];

export async function getUsers(): Promise<ApiResponse<User[]>> {
  return { data: users, total: users.length };
}

export async function createUser(input: Partial<User>): Promise<ApiResponse<User>> {
  const user: User = {
    id: crypto.randomUUID(),
    name: input.name || 'New User',
    email: input.email || '',
    role: input.role || 'RESEARCHER',
    status: 'active',
    createdAt: new Date().toISOString(),
  };
  users.push(user);
  return { data: user, total: users.length };
}
`,
      "functions/types.ts": `export interface User {
  id: string;
  name: string;
  email: string;
  role: string;
  status: string;
  createdAt: string;
}

export interface ApiResponse<T> {
  data: T;
  total: number;
}

export interface ErrorResponse {
  error: string;
  details?: string;
}
`,
    },
  },
  {
    id: "atlas-plugin",
    name: "Atlas Plugin",
    description: "OHDSI Atlas single-spa plugin with Vue 3 + Vuetify 3 + WebAPI",
    tech_stack: "atlas-vue",
    dev_command: "npm run dev",
    install_command: "npm install",
    build_command: "npm run build",
    files: {
      "AI_RULES.md": `# Atlas Plugin — Tech Stack & Design System

## Design References
- See \`ATLAS_DESIGN_SYSTEM.md\` for visual patterns, component styling, and Atlas-specific conventions
- See \`MATERIAL_DESIGN_GUIDELINES.md\` for UI patterns not covered by Atlas (dialogs, forms, loading, etc.)
- See \`WEBAPI_API_REFERENCE.md\` for all available REST API endpoints


## Tech Stack
- Vue 3.4 + Vuetify 3.5 + Vite + TypeScript (\`<script setup lang="ts">\`)
- Pinia for state management, Vue Router 4 for navigation
- single-spa-vue for plugin lifecycle (production entry: src/lifecycles.ts)
- @mdi/font for Material Design Icons (use via Vuetify: \`mdi-{icon-name}\`)
- Font: Roboto (Vuetify default — no custom import needed)

## Project Structure
- \`src/views/\` — page components (HomeView.vue, etc.)
- \`src/components/\` — reusable components
- \`src/stores/\` — Pinia stores
- \`src/composables/\` — composition functions (usePluginProps, useWebApi)
- \`src/router/\` — Vue Router config
- \`src/plugins/vuetify.ts\` — Vuetify theme (DO NOT modify colors)
- UPDATE \`src/views/HomeView.vue\` or add new views. The App.vue routes to views via router.

## Atlas Theme Colors (MUST use exactly these)
- primary: \`#1f425a\` (dark blue — Atlas brand)
- secondary: \`#424242\`
- accent: \`#2d5f7f\` (lighter blue)
- error: \`#FF5252\`, info: \`#2196F3\`, success: \`#4CAF50\`, warning: \`#FB8C00\`
- orange: \`#eb6622\` (accent for CTAs and highlights)
- background: \`#f2f0f1\` (light grey — ALL page backgrounds)
- surface: \`#FFFFFF\` (cards, dialogs)

## Vuetify Component Defaults (already configured in vuetify.ts)
- VBtn: flat variant, primary color
- VCard: elevated variant, elevation 2
- VTextField / VSelect / VAutocomplete: outlined variant, comfortable density

## Page Layout Pattern (MUST follow for every page)
\`\`\`vue
<template>
  <div class="page-wrapper">
    <div class="page-card">
      <v-container fluid class="pa-0">
        <!-- page content here -->
      </v-container>
    </div>
  </div>
</template>

<style scoped>
.page-wrapper {
  min-height: 100%;
  background: rgb(var(--v-theme-background));
  padding: 32px;
  display: flex;
  flex-direction: column;
}
.page-card {
  background: #fff;
  border-radius: 18px;
  padding: 30px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.08);
  flex: 1;
}
</style>
\`\`\`

## Plugin Props (available via usePluginProps composable)
- \`getToken()\` — returns JWT for Authorization header
- \`username\` — current user
- \`datasetId\` — selected dataset/source key
- \`messageBus\` — host communication (send, request, subscribe)
- \`locale\` — user language

## API Calls (IMPORTANT)
- NO backend — use WebAPI REST endpoints directly
- See \`WEBAPI_API_REFERENCE.md\` for ALL available endpoints
- Use the \`useWebApi()\` composable for all API calls — it handles base URL and auth
- WebAPI URL is configurable via \`VITE_WEBAPI_URL\` in \`.env\` (default: http://localhost:8080/WebAPI)
- In dev mode, Vite proxies /WebAPI requests to avoid CORS
- Auth: \`Authorization: Bearer \${await getToken()}\` (handled by useWebApi)

## Common Icons (Material Design)
mdi-plus, mdi-magnify, mdi-delete, mdi-pencil, mdi-information-outline,
mdi-account-multiple, mdi-content-copy, mdi-download, mdi-upload,
mdi-filter, mdi-sort, mdi-refresh, mdi-chevron-right, mdi-close
`,
      "ATLAS_DESIGN_SYSTEM.md": `# Atlas Design System

Visual patterns and component styling conventions extracted from Atlas3 source code.
All values verified against the real Atlas3 codebase.

---

## 1. Font

Vuetify uses **Roboto** by default — no custom font import needed.

## 2. Color Usage Guidelines

Use the Atlas theme colors consistently. Here is when to use each:

| Color | Hex | Usage |
|-------|-----|-------|
| primary | \`#1f425a\` | Headings, nav links, data values, primary buttons, page text |
| accent | \`#2d5f7f\` | Hover states, secondary headings, link hover color |
| orange | \`#eb6622\` | CTA buttons, highlight borders, attention-drawing elements |
| Material Blue | \`#1976d2\` | Interactive hover borders, count highlights, progress bars |
| secondary text | \`#666\` | Labels, meta info, secondary descriptions |
| tertiary text | \`#999\` | Subtle labels, separators, patient count labels |
| border grey | \`#e0e0e0\` | Borders, dividers, inactive tile borders |
| background | \`#f2f0f1\` | Page backgrounds (via theme) |
| surface | \`#FFFFFF\` | Cards, dialogs, navbar |

**Tonal variants**: Use Vuetify \`variant="tonal"\` with semantic colors for status chips and alerts (e.g., \`<v-chip color="success" variant="tonal">\`).

## 3. Typography

Font: Roboto (Vuetify default)

| Level | Size | Weight | Usage |
|-------|------|--------|-------|
| h1 / display | 4rem | 300 | Landing page titles only, letter-spacing 0.2em |
| h2 / section title | 1.5rem | 600 | Section headings, color: primary |
| h3 | 1.1rem | 500 | Sub-section headings |
| Body | 16px | 400 | Default text, line-height 1.6 |
| Body small | 0.875rem (14px) | 400 | Secondary text, table cells |
| Caption | 0.75rem (12px) | 400 | Meta labels, source keys, color: #666 |

Use Vuetify text classes: \`text-h5\`, \`text-body-1\`, \`text-body-2\`, \`text-caption\`, \`font-weight-bold\`, \`text-medium-emphasis\`.

## 4. Spacing

- Page outer padding: **32px**
- Card internal padding: **30px**
- Section gaps: **16px**
- Form field gaps: **8px–12px**
- Always use **multiples of 8px**
- Vuetify spacing classes: \`pa-3\` (12px), \`pa-4\` (16px), \`mb-2\` (8px), \`mb-4\` (16px), \`ga-2\` (8px gap), \`ga-4\` (16px gap)

## 5. Border Radius

- Page cards / containers: **18px**
- Buttons: **4px**
- Data source tiles: **4px**
- Small components (chips, badges): **3–4px**
- Nav active underline: **0.5rem 0.5rem 0 0** (top corners only)

## 6. Borders & Dividers

- Standard divider: \`1px solid #e0e0e0\`
- Tile border (idle): \`1px solid #e0e0e0\`
- Tile border (hover): \`border-color: #1976d2\`
- Tile border (complete): \`border-color: #4caf50\`
- Outlined button border: \`2px solid\`
- Tile header separator: \`border-bottom: 1px solid #e0e0e0\`

## 7. Shadows & Elevation

| Element | Shadow |
|---------|--------|
| Page cards | \`0 2px 4px rgba(0, 0, 0, 0.08)\` |
| NavBar | \`0 2px 8px rgba(0, 0, 0, 0.1)\` |
| Card hover | \`0 4px 8px rgba(0, 0, 0, 0.1)\` |
| Tile hover | \`0 2px 4px rgba(0, 0, 0, 0.1)\` |

Vuetify elevation: prefer **0–4**. Cards default to elevation 2. Event cards use elevation 1, hover to 4.

## 8. Hover & Interaction States

- **Card hover**: elevation 1 → 4, transition \`all 0.3s ease\`
- **Tile hover**: border-color changes to \`#1976d2\`, adds subtle shadow
- **Complete tile hover**: background \`#f1f8e9\` (light green)
- **Link hover**: color changes to accent, transition \`0.15s ease-in-out\`
- **Outline button hover**: fills with primary color, text turns white
- **Secondary button hover**: fills with orange, text turns white
- All transitions: \`all 0.2s ease-in-out\` (buttons), \`all 0.3s ease\` (cards)

## 9. Status Colors & Indicators

| Status | Color | Background | Icon |
|--------|-------|------------|------|
| Complete | \`#4caf50\` (success) | \`#f1f8e9\` | — |
| Running / Generating | primary | — | \`v-progress-circular\` size=20 width=2 |
| Error / Failed | \`#FF5252\` (error) | — | \`mdi-alert-circle\` |
| Idle | — | — | — |
| Cache ready | success | tonal | — |
| Cache building | info | tonal | — |
| Cache stale | warning | tonal | \`mdi-clock-alert\` |

Use \`<v-chip color="..." variant="tonal" size="x-small">\` for status indicators.

## 10. NavBar & Navigation Patterns

- Height: **56px**, white background, shadow \`0 2px 8px rgba(0, 0, 0, 0.1)\`
- Nav links: font-size **16px**, padding **18px 12px**, color: primary, font-weight 400
- Nav link hover: color changes to accent, transition 0.15s
- Responsive: on 960–1279px, nav links shrink to 14px with 8px horizontal padding
- Active indicator: bottom underline bar — height **0.5rem**, primary color, border-radius **0.5rem 0.5rem 0 0**, font-weight 500
- Hide nav links below **960px** (use Vuetify \`d-none d-md-flex\`)
- Logo area: left-aligned, 52px height, with cursor pointer

## 11. Buttons

| Type | Style | Hover |
|------|-------|-------|
| Primary | \`variant="flat" color="primary"\` (Vuetify default) | Built-in Vuetify hover |
| Outline | \`border: 2px solid primary\`, transparent bg, primary text | Fills with primary, white text |
| Secondary / CTA | \`border: 2px solid orange\`, white bg, primary text | Fills with orange, white text |
| Text | \`variant="text"\` | Subtle background |
| Icon | \`variant="text" size="small"\` | Subtle background |

- Min-width for landing buttons: **180px**
- Button padding: **0.75rem 1.5rem**, font-weight 500, font-size 16px
- Border-radius: **4px**
- Transition: \`all 0.2s ease-in-out\`

## 12. Data Tables

- Use \`<v-data-table>\` with \`density="comfortable"\` and \`:elevation="0"\`
- Search bar: \`<v-text-field>\` outlined, max-width 400px, \`prepend-inner-icon="mdi-magnify"\`, hide-details
- Loading: \`<v-skeleton-loader type="table-row@10" />\`
- Empty: \`<v-alert type="info" variant="tonal">No data available</v-alert>\`

## 13. Atlas Component Patterns

### Data Source Tiles
- Border: \`1px solid #e0e0e0\`, border-radius 4px
- Hover: border-color \`#1976d2\`, shadow \`0 2px 4px rgba(0, 0, 0, 0.1)\`
- Complete state: border-color \`#4caf50\`, cursor pointer, hover bg \`#f1f8e9\`
- Header: source name (0.875rem, weight 500, primary color) + source key (0.75rem, #666)
- Header separator: \`border-bottom: 1px solid #e0e0e0\`, padding-bottom 8px
- Grid: use Vuetify grid with cols 12/6/4/3 for responsive layout

### Patient Count Bar
- Background: \`linear-gradient(to right, #f8f9fa, #ffffff)\`
- Border-bottom: \`1px solid #e0e0e0\`, padding 12px 24px
- Count number: font-size **20px**, weight 600, color \`#1976d2\`
- Count label: font-size **12px**, color \`#999\`
- Separator: font-size 16px, color \`#999\`
- Total count: font-size 16px, weight 500, color \`#666\`
- Progress bar: \`<v-progress-linear>\` height 8, rounded

### Event Cards
- Elevation 1, transition \`all 0.3s ease\`
- Hover: shadow \`0 4px 8px rgba(0, 0, 0, 0.1)\`
- Header: icon (small) + title (text-subtitle-1) + caption (text-caption, text-medium-emphasis)
- Summary chips: \`<v-chip size="small" color="primary" variant="tonal">\` with icons
- Expanded section: \`<v-expand-transition>\` with divider separator
- Action buttons inside: \`variant="outlined" size="small"\` with prepend-icon

## 14. Card Patterns

- Title: mdi icon (18px, primary color) + text
- Description: truncated with \`<v-tooltip>\`
- Meta info: 2-column rows (ID, author, created, updated)
- Tags: \`<v-chip>\` with custom colors
- Actions: icon buttons (\`size="small"\`, \`variant="text"\`)

## 15. Responsive Breakpoints & Grid

Vuetify breakpoints:
| Name | Range |
|------|-------|
| xs | < 600px |
| sm | 600–959px |
| md | 960–1263px |
| lg | 1264–1903px |
| xl | ≥ 1904px |

Grid patterns:
- Landing page: \`grid-template-columns: 1fr 400px\` → single column on small screens
- Data source tiles: Vuetify grid cols 12/6/4/3 (1/2/3/4 columns)
- Max content width: 940px (landing), 1400px (count bar)
- Use Vuetify display classes: \`d-none d-md-flex\`, \`d-sm-none\`
`,
      "MATERIAL_DESIGN_GUIDELINES.md": `# Material Design Guidelines

Gap-fill for UI patterns that Atlas does not explicitly define.
Follow these Material Design conventions for consistency with the Vuetify framework.

---

## 1. Dialogs

- Small dialogs: \`max-width="560"\`
- Medium dialogs: \`max-width="800"\`
- Internal padding: **24px** (\`pa-6\`)
- Title: font-size 20px (text-h6), font-weight 500
- Actions: right-aligned, 8px gap between buttons
- Use \`persistent\` prop for destructive or important actions
- Destructive confirm: secondary button "Cancel" + error-colored "Delete"

\`\`\`vue
<v-dialog max-width="560">
  <v-card>
    <v-card-title class="text-h6">{{ t('myPlugin.dialog.title', 'Dialog Title') }}</v-card-title>
    <v-card-text>{{ t('myPlugin.dialog.message', 'Content here') }}</v-card-text>
    <v-card-actions class="justify-end ga-2 pa-4">
      <v-btn variant="text" @click="close">{{ t('common.cancel', 'Cancel') }}</v-btn>
      <v-btn color="primary" @click="confirm">{{ t('common.confirm', 'Confirm') }}</v-btn>
    </v-card-actions>
  </v-card>
</v-dialog>
\`\`\`

## 2. Snackbar / Toast Notifications

- Position: bottom center (\`location="bottom"\`)
- Duration: **4 seconds** for info, **8 seconds** for errors
- Single action button (e.g., "Dismiss", "Undo")
- Use semantic colors: success, error, info, warning

\`\`\`vue
<v-snackbar v-model="snackbar" :timeout="4000" color="success" location="bottom">
  {{ message }}
  <template #actions>
    <v-btn variant="text" @click="snackbar = false">{{ t('common.close', 'Close') }}</v-btn>
  </template>
</v-snackbar>
\`\`\`

## 3. Empty States

- Centered layout with icon + title + subtitle + optional action
- Icon: size **48px**, color **#999**
- Title: text-h6, font-weight 500
- Subtitle: text-body-2, color #666
- Action: primary button below subtitle

\`\`\`vue
<div class="d-flex flex-column align-center justify-center pa-12 text-center">
  <v-icon size="48" color="grey">mdi-database-off</v-icon>
  <h3 class="text-h6 mt-4">{{ t('myPlugin.empty.title', 'No Data Found') }}</h3>
  <p class="text-body-2 mt-2" style="color: #666">
    {{ t('myPlugin.empty.subtitle', 'There are no items matching your criteria.') }}
  </p>
  <v-btn class="mt-4" prepend-icon="mdi-plus">{{ t('myPlugin.actions.createNew', 'Create New') }}</v-btn>
</div>
\`\`\`

## 4. Loading States

| Context | Component | Props |
|---------|-----------|-------|
| Table loading | \`<v-skeleton-loader>\` | \`type="table-row@10"\` |
| Card loading | \`<v-skeleton-loader>\` | \`type="card"\` |
| Inline action | \`<v-progress-circular>\` | \`size="20" width="2" indeterminate\` |
| Full page | \`<v-progress-circular>\` | \`size="48" indeterminate\`, centered |
| List loading | \`<v-skeleton-loader>\` | \`type="list-item-two-line@5"\` |

Full page loading pattern:
\`\`\`vue
<div class="d-flex align-center justify-center" style="min-height: 200px">
  <v-progress-circular size="48" indeterminate color="primary" />
</div>
\`\`\`

## 5. Form Patterns

- Labels: above inputs (Vuetify outlined variant, already configured as default)
- Field spacing: **16px** between fields (\`mb-4\` class)
- Helper text: use Vuetify \`hint\` prop with \`persistent-hint\`
- Validation: use Vuetify \`rules\` prop for inline validation
- Actions: right-aligned, Cancel (text variant) + Submit (primary flat)
- Group related fields in sections with \`text-subtitle-2\` headings and \`mb-6\` spacing

\`\`\`vue
<v-form @submit.prevent="handleSubmit">
  <v-text-field v-model="name" :label="tv('myPlugin.form.name', 'Name')" :rules="[v => !!v || tv('common.required', 'Required')]" class="mb-4" />
  <v-text-field v-model="description" :label="tv('myPlugin.form.description', 'Description')" :hint="tv('common.optional', 'Optional')" persistent-hint class="mb-4" />
  <div class="d-flex justify-end ga-2 mt-6">
    <v-btn variant="text" @click="cancel">{{ t('common.cancel', 'Cancel') }}</v-btn>
    <v-btn type="submit" :loading="saving">{{ t('common.save', 'Save') }}</v-btn>
  </div>
</v-form>
\`\`\`

## 6. Chip Patterns

| Use Case | Variant | Size | Props |
|----------|---------|------|-------|
| Status indicators | tonal | small | \`color="success/error/warning"\` |
| Filters (removable) | outlined | default | \`closable\` |
| Category tags | tonal | small | \`color="primary/accent"\` |
| Counts / badges | tonal | x-small | with icon |

## 7. Transitions & Motion

| Type | Duration | Easing | Example |
|------|----------|--------|---------|
| Simple (hover, color) | 0.15–0.2s | ease-in-out | Link hover, button hover |
| Medium (expand, slide) | 0.3s | ease | Card expand, panel open |
| Complex (multi-step) | Use Vuetify built-in | — | \`<v-expand-transition>\`, \`<v-fade-transition>\` |

Prefer Vuetify built-in transitions:
- \`<v-expand-transition>\` for collapsible content
- \`<v-fade-transition>\` for appearance/disappearance
- \`<v-slide-y-transition>\` for vertical entry
- \`<v-scale-transition>\` for dialogs and FABs

## 8. Accessibility

- Text contrast: **4.5:1** minimum (WCAG AA)
- Large text (18px+ or 14px+ bold): **3:1** minimum
- Icon-only buttons: always add \`aria-label\`
- Keyboard navigation: do NOT override Vuetify's built-in focus rings
- Form inputs: always provide \`label\` prop (even if visually hidden)
- Use semantic HTML: headings in order, lists for navigation
- Disabled states: use Vuetify \`disabled\` prop (handles aria automatically)
`,
      "WEBAPI_API_REFERENCE.md": `# WebAPI REST API Reference

Base URL: Configured via \`VITE_WEBAPI_URL\` (default: \`http://localhost:8080/WebAPI\`)
Auth: \`Authorization: Bearer <JWT>\` header on all requests (handled by useWebApi composable)

---

## Cohort Definitions

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | \`/cohortdefinition\` | List all cohort definitions |
| POST | \`/cohortdefinition\` | Create new cohort definition |
| GET | \`/cohortdefinition/{id}\` | Get cohort by ID |
| PUT | \`/cohortdefinition/{id}\` | Update cohort |
| DELETE | \`/cohortdefinition/{id}\` | Delete cohort |
| GET | \`/cohortdefinition/{id}/generate/{sourceKey}\` | Generate cohort for source |
| GET | \`/cohortdefinition/{id}/report/{sourceKey}\` | Get generation report |
| POST | \`/cohortdefinition/sql\` | Generate SQL from cohort JSON |
| GET | \`/cohortdefinition/{id}/copy\` | Copy cohort definition |
| GET | \`/cohortdefinition/{id}/info\` | Get cohort metadata |

## Concept Sets

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | \`/conceptset\` | List all concept sets |
| POST | \`/conceptset\` | Create concept set |
| GET | \`/conceptset/{id}\` | Get concept set by ID |
| PUT | \`/conceptset/{id}\` | Update concept set |
| DELETE | \`/conceptset/{id}\` | Delete concept set |
| GET | \`/conceptset/{id}/items\` | Get concept set items |
| PUT | \`/conceptset/{id}/items\` | Update items |
| GET | \`/conceptset/{id}/expression\` | Get expression |

## Vocabulary Search

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | \`/vocabulary/{sourceKey}/search\` | Search concepts (body: {QUERY, DOMAIN_ID[], ...}) |
| GET | \`/vocabulary/{sourceKey}/concept/{id}\` | Get concept details |
| GET | \`/vocabulary/{sourceKey}/concept/{id}/related\` | Get related concepts |
| GET | \`/vocabulary/{sourceKey}/concept/{id}/descendants\` | Get descendants |
| GET | \`/vocabulary/{sourceKey}/domains\` | List available domains |
| GET | \`/vocabulary/{sourceKey}/vocabularies\` | List vocabularies |
| POST | \`/vocabulary/{sourceKey}/resolveConceptSetExpression\` | Resolve expression |
| POST | \`/vocabulary/{sourceKey}/lookup/identifiers\` | Lookup by concept IDs |

## Data Sources

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | \`/source/sources\` | List all data sources |
| GET | \`/source/{key}\` | Get source by key |
| GET | \`/source/details/{sourceId}\` | Get source details |
| GET | \`/source/connection/{key}\` | Test connection |

## CDM Results & Reports

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | \`/cdmresults/{sourceKey}/dashboard\` | Dashboard summary |
| GET | \`/cdmresults/{sourceKey}/person\` | Person/patient statistics |
| GET | \`/cdmresults/{sourceKey}/datadensity\` | Data density report |
| GET | \`/cdmresults/{sourceKey}/death\` | Mortality statistics |
| GET | \`/cdmresults/{sourceKey}/observationPeriod\` | Observation periods |
| GET | \`/cdmresults/{sourceKey}/{domain}/\` | Domain treemap |
| GET | \`/cdmresults/{sourceKey}/{domain}/{conceptId}\` | Concept drilldown |
| POST | \`/cdmresults/{sourceKey}/conceptRecordCount\` | Record counts |

## Authentication

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | \`/user/me\` | Get current user info |
| GET | \`/user/refresh\` | Refresh JWT token |
| GET | \`/user/logout\` | Logout |
| GET | \`/user/login/db\` | Database login |

## Common Patterns

- **sourceKey**: String identifier for a data source (e.g. "OHDSI-CDMV5")
- **Pagination**: Most list endpoints support query params for paging
- **Content-Type**: Always \`application/json\`
- **Errors**: HTTP status codes with JSON body \`{ "message": "..." }\`
`,
      ".env": `VITE_WEBAPI_URL=http://localhost:8080/WebAPI
`,
      "package.json": `{
  "name": "@trex/__APP_ID__",
  "private": true,
  "version": "0.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "vue": "^3.4.0",
    "vuetify": "^3.5.0",
    "@mdi/font": "^7.4.47",
    "vue-router": "^4.2.0",
    "pinia": "^2.1.0",
    "single-spa-vue": "^3.0.1"
  },
  "devDependencies": {
    "@vitejs/plugin-vue": "^5.2.1",
    "vite-plugin-vuetify": "^2.0.4",
    "vite-plugin-css-injected-by-js": "^3.5.2",
    "typescript": "~5.6.2",
    "vite": "^6.0.1",
    "vue-tsc": "^2.1.10"
  },
  "trex": {
    "ui": {
      "routes": [
        {
          "path": "/app",
          "dir": "dist",
          "spa": true
        }
      ]
    }
  }
}`,
      "vite.config.ts": `import { defineConfig, loadEnv } from 'vite';
import vue from '@vitejs/plugin-vue';
import vuetify from 'vite-plugin-vuetify';
import cssInjectedByJsPlugin from 'vite-plugin-css-injected-by-js';

export default defineConfig(({ command, mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const webapiUrl = env.VITE_WEBAPI_URL || 'http://localhost:8080/WebAPI';

  // Parse the URL to extract origin and path for proxy configuration
  let proxyTarget = 'http://localhost:8080';
  let proxyRewrite: Record<string, string> = {};
  try {
    const parsed = new URL(webapiUrl);
    proxyTarget = parsed.origin;
    const remotePath = parsed.pathname.replace(/\\/$/, '');
    if (remotePath && remotePath !== '/WebAPI') {
      proxyRewrite = { '^/WebAPI': remotePath };
    }
  } catch { /* use defaults */ }

  return {
  plugins: [vue(), vuetify({ autoImport: true }), cssInjectedByJsPlugin()],
  resolve: {
    alias: { '@': '/src' },
  },
  server: {
    proxy: {
      '/WebAPI': {
        target: proxyTarget,
        changeOrigin: true,
        ...(Object.keys(proxyRewrite).length > 0 ? { rewrite: (path) => {
          for (const [from, to] of Object.entries(proxyRewrite)) {
            path = path.replace(new RegExp(from), to);
          }
          return path;
        }} : {}),
      },
    },
  },
  build: command === 'build' ? {
    lib: {
      entry: 'src/lifecycles.ts',
      formats: ['system'],
      fileName: () => 'app.js',
    },
    rollupOptions: {
      external: ['vue', 'vue-router', 'pinia'],
    },
    cssCodeSplit: false,
  } : undefined,
};
});`,
      "tsconfig.json": `{
  "files": [],
  "references": [
    { "path": "./tsconfig.app.json" },
    { "path": "./tsconfig.node.json" }
  ]
}`,
      "tsconfig.app.json": `{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "module": "ESNext",
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "moduleDetection": "force",
    "noEmit": true,
    "jsx": "preserve",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "paths": { "@/*": ["./src/*"] }
  },
  "include": ["src/**/*.ts", "src/**/*.tsx", "src/**/*.vue", "src/**/*.json"]
}`,
      "tsconfig.node.json": `{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2023"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "isolatedModules": true,
    "moduleDetection": "force",
    "noEmit": true,
    "strict": true
  },
  "include": ["vite.config.ts"]
}`,
      "index.html": `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Atlas Plugin</title>
  </head>
  <body>
    <div id="app"></div>
    <script type="module" src="/src/main.ts"></script>
  </body>
</html>`,
      "src/plugins/vuetify.ts": `import 'vuetify/styles';
import '@mdi/font/css/materialdesignicons.css';
import { createVuetify } from 'vuetify';

export default createVuetify({
  theme: {
    defaultTheme: 'atlas',
    themes: {
      atlas: {
        dark: false,
        colors: {
          primary: '#1f425a',
          secondary: '#424242',
          accent: '#2d5f7f',
          error: '#FF5252',
          info: '#2196F3',
          success: '#4CAF50',
          warning: '#FB8C00',
          orange: '#eb6622',
          background: '#f2f0f1',
          surface: '#FFFFFF',
        },
      },
    },
  },
  defaults: {
    VBtn: { variant: 'flat', color: 'primary' },
    VCard: { variant: 'elevated', elevation: 2 },
    VTextField: { variant: 'outlined', density: 'comfortable' },
    VSelect: { variant: 'outlined', density: 'comfortable' },
    VAutocomplete: { variant: 'outlined', density: 'comfortable' },
  },
});`,
      "src/router/index.ts": `import { createRouter, createWebHashHistory } from 'vue-router';
import HomeView from '@/views/HomeView.vue';

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', name: 'home', component: HomeView },
  ],
});

export default router;`,
      "src/composables/usePluginProps.ts": `import { inject, ref } from 'vue';

export interface PluginProps {
  getToken: () => Promise<string>;
  username: string;
  datasetId: string;
  messageBus?: any;
  locale?: string;
}

const PLUGIN_PROPS_KEY = Symbol('pluginProps');

export function providePluginProps(props: PluginProps) {
  return { key: PLUGIN_PROPS_KEY, value: props };
}

export function usePluginProps(): PluginProps {
  const props = inject<PluginProps>(PLUGIN_PROPS_KEY);
  if (props) return props;

  // Dev mode fallback
  return {
    getToken: async () => 'dev-token',
    username: 'developer',
    datasetId: 'OHDSI-CDMV5',
    locale: 'en',
  };
}`,
      "src/stores/locale.ts": `import { defineStore } from 'pinia';
import { useWebApi } from '@/composables/useWebApi';
import fallbackTranslations from '@/locales/en.json';

export type Translations = Record<string, unknown>;

interface TranslationCache {
  translations: Translations;
  cachedAt: number;
}

const CACHE_MAX_AGE = 24 * 60 * 60 * 1000; // 24 hours

export const useLocaleStore = defineStore('locale', {
  state: () => ({
    locale: 'en' as string,
    translations: {} as Translations,
    loading: false,
    initialized: false,
    translationCache: new Map<string, TranslationCache>(),
  }),

  actions: {
    async initialize(locale?: string): Promise<void> {
      // Load bundled English fallback immediately
      this.translations = fallbackTranslations as Translations;

      // Cache the fallback
      this.translationCache.set('en', {
        translations: this.translations,
        cachedAt: Date.now(),
      });

      const targetLocale = locale || 'en';

      // Try to fetch from WebAPI (backend is the source of truth)
      await this.fetchTranslations(targetLocale);
      this.locale = targetLocale;
      this.initialized = true;
    },

    async fetchTranslations(locale: string): Promise<void> {
      // Check memory cache first
      const cached = this.translationCache.get(locale);
      if (cached && Date.now() - cached.cachedAt < CACHE_MAX_AGE) {
        this.translations = cached.translations;
        return;
      }

      this.loading = true;
      try {
        const { webApiFetch } = useWebApi();
        const data = await webApiFetch<Translations>(\`/i18n?lang=\${locale}\`);
        this.translations = data;
        this.translationCache.set(locale, {
          translations: data,
          cachedAt: Date.now(),
        });
      } catch {
        // Backend unavailable — keep using fallback translations
        console.warn(\`[i18n] Could not fetch translations for "\${locale}", using fallback\`);
      } finally {
        this.loading = false;
      }
    },

    async changeLocale(locale: string): Promise<void> {
      await this.fetchTranslations(locale);
      this.locale = locale;
    },
  },
});`,
      "src/locales/en.json": `{
  "common": {
    "add": "Add",
    "cancel": "Cancel",
    "close": "Close",
    "copy": "Copy",
    "create": "Create",
    "delete": "Delete",
    "description": "Description",
    "download": "Download",
    "export": "Export",
    "failed": "Failed",
    "import": "Import",
    "loading": "Loading...",
    "noData": "No data",
    "optional": "Optional",
    "patients": "Patients",
    "preview": "Preview",
    "refresh": "Refresh",
    "required": "Required",
    "retry": "Retry",
    "save": "Save",
    "search": "Search",
    "confirm": "Confirm"
  },
  "datatable": {
    "emptyTable": "No data available",
    "search": "Search...",
    "noMatchingRecords": "No matching records found"
  }
}`,
      "src/composables/useI18n.ts": `import { computed } from 'vue';
import type { ComputedRef } from 'vue';
import { useLocaleStore } from '@/stores/locale';

export type TranslationParams = Record<string, string | number>;

function getNestedValue(obj: Record<string, unknown>, path: string): string | undefined {
  const keys = path.split('.');
  let value: unknown = obj;
  for (const key of keys) {
    if (value === undefined || value === null) return undefined;
    value = (value as Record<string, unknown>)[key];
  }
  return typeof value === 'string' ? value : undefined;
}

function interpolate(template: string, params: TranslationParams): string {
  return template.replace(/\\{(\\w+)\\}/g, (match, key) => {
    return params[key] !== undefined ? String(params[key]) : match;
  });
}

function resolve(
  store: ReturnType<typeof useLocaleStore>,
  key: string,
  defaultValueOrParams?: string | TranslationParams,
  params?: TranslationParams,
): string {
  let defaultValue = '';
  let tParams: TranslationParams | undefined;

  if (typeof defaultValueOrParams === 'object') {
    tParams = defaultValueOrParams;
  } else if (typeof defaultValueOrParams === 'string') {
    defaultValue = defaultValueOrParams;
    tParams = params;
  }

  // Lookup in current translations (backend source or fallback)
  let translation = getNestedValue(store.translations, key);

  // Fallback to English cache if current locale differs
  if (!translation && store.locale !== 'en') {
    const enCache = store.translationCache.get('en');
    if (enCache) {
      translation = getNestedValue(enCache.translations, key);
    }
  }

  // Fallback to provided default value, then to key itself
  if (!translation) {
    translation = defaultValue || key;
  }

  return tParams ? interpolate(translation, tParams) : translation;
}

/**
 * i18n composable — translations are fetched from the WebAPI backend.
 * The bundled en.json provides a fallback when the backend is unavailable.
 *
 * Fallback chain: backend translations → English cache → default value → key
 */
export function useI18n() {
  const store = useLocaleStore();

  /** Reactive translation (ComputedRef) — use in templates: {{ t('key', 'Default') }} */
  const t = (
    key: string,
    defaultValueOrParams?: string | TranslationParams,
    params?: TranslationParams,
  ): ComputedRef<string> => computed(() => resolve(store, key, defaultValueOrParams, params));

  /** Non-reactive translation (string) — use in v-bind, props, function args */
  const tv = (
    key: string,
    defaultValueOrParams?: string | TranslationParams,
    params?: TranslationParams,
  ): string => resolve(store, key, defaultValueOrParams, params);

  return {
    t,
    tv,
    locale: computed(() => store.locale),
    loading: computed(() => store.loading),
    changeLocale: (locale: string) => store.changeLocale(locale),
  };
}`,
      "src/composables/useWebApi.ts": `import { usePluginProps } from './usePluginProps';

// In dev mode, always use the proxy path — Vite proxies /WebAPI to the real server.
// In production (single-spa), use the full URL from env or plugin config.
const WEBAPI_BASE = import.meta.env.DEV ? '/WebAPI' : (import.meta.env.VITE_WEBAPI_URL || '/WebAPI');

export function useWebApi() {
  const { getToken } = usePluginProps();

  async function webApiFetch<T = any>(path: string, options?: RequestInit): Promise<T> {
    const token = await getToken();
    // Ensure no double slashes between base and path
    const url = \`\${WEBAPI_BASE.replace(/\\/$/, '')}\${path.startsWith('/') ? path : '/' + path}\`;
    const res = await fetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': \`Bearer \${token}\`,
        ...options?.headers,
      },
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(\`WebAPI error \${res.status}: \${body}\`);
    }
    return res.json();
  }

  return { webApiFetch, baseUrl: WEBAPI_BASE };
}`,
      "src/lifecycles.ts": `import singleSpaVue from 'single-spa-vue';
import { createApp, h } from 'vue';
import { createPinia } from 'pinia';
import vuetify from './plugins/vuetify';
import router from './router';
import App from './App.vue';
import { providePluginProps } from './composables/usePluginProps';
import { useLocaleStore } from './stores/locale';

const vueLifecycles = singleSpaVue({
  createApp,
  appOptions: {
    render() {
      return h(App);
    },
  },
  handleInstance(app, props) {
    app.use(vuetify);
    app.use(createPinia());
    app.use(router);

    const pluginProps = providePluginProps({
      getToken: props.getToken || (async () => ''),
      username: props.username || '',
      datasetId: props.datasetId || '',
      messageBus: props.messageBus,
      locale: props.locale || 'en',
    });
    app.provide(pluginProps.key, pluginProps.value);

    // Initialize i18n — fetches translations from backend, falls back to bundled en.json
    const localeStore = useLocaleStore();
    localeStore.initialize(props.locale || 'en');
  },
});

export const bootstrap = vueLifecycles.bootstrap;
export const mount = vueLifecycles.mount;
export const unmount = vueLifecycles.unmount;`,
      "src/main.ts": `import { createApp } from 'vue';
import { createPinia } from 'pinia';
import vuetify from './plugins/vuetify';
import router from './router';
import App from './App.vue';
import { providePluginProps } from './composables/usePluginProps';
import { useLocaleStore } from './stores/locale';

const app = createApp(App);
app.use(vuetify);
app.use(createPinia());
app.use(router);

// Provide mock plugin props for dev mode
const mockProps = providePluginProps({
  getToken: async () => 'dev-token',
  username: 'developer',
  datasetId: 'OHDSI-CDMV5',
  locale: 'en',
});
app.provide(mockProps.key, mockProps.value);

// Initialize i18n — fetches translations from backend, falls back to bundled en.json
const localeStore = useLocaleStore();
localeStore.initialize('en');

app.mount('#app');`,
      "src/App.vue": `<script setup lang="ts">
import { computed } from 'vue';
import AtlasShell from '@/components/AtlasShell.vue';

// AtlasShell is only shown in dev mode (standalone).
// In production (single-spa), the Atlas host provides the shell.
const isDev = computed(() => import.meta.env.DEV);
</script>

<template>
  <v-app>
    <AtlasShell v-if="isDev" />
    <v-main style="background: rgb(var(--v-theme-background))">
      <router-view />
    </v-main>
  </v-app>
</template>`,
      "src/components/AtlasShell.vue": `<script setup lang="ts">
/**
 * Dev-only Atlas host shell — mimics the Atlas3 NavBar for preview.
 * Uses plain HTML to avoid Vuetify v-app-bar scope warnings in dev mode.
 * This component is NOT included in the production single-spa build.
 */
const navItems = ['Data Sources', 'Concept Sets', 'Cohorts'];
const webapiUrl = import.meta.env.VITE_WEBAPI_URL || 'http://localhost:8080/WebAPI';
</script>

<template>
  <header class="atlas-shell-nav">
    <div class="atlas-shell-nav__left">
      <span class="atlas-shell-nav__logo">ATLAS</span>
      <span class="atlas-shell-nav__badge">DEV PREVIEW</span>
    </div>
    <nav class="atlas-shell-nav__links">
      <a
        v-for="item in navItems"
        :key="item"
        href="#"
        class="atlas-shell-nav__link"
        @click.prevent
      >
        {{ item }}
      </a>
    </nav>
    <div class="atlas-shell-nav__right">
      <span class="atlas-shell-nav__api">{{ webapiUrl }}</span>
    </div>
  </header>
</template>

<style scoped>
.atlas-shell-nav {
  width: 100%;
  height: 56px;
  background-color: #ffffff;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  display: flex;
  align-items: center;
  padding: 0 1rem;
  position: sticky;
  top: 0;
  z-index: 1000;
}
.atlas-shell-nav__left {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}
.atlas-shell-nav__logo {
  font-size: 1.5rem;
  font-weight: 300;
  letter-spacing: 0.2em;
  color: rgb(var(--v-theme-primary));
}
.atlas-shell-nav__badge {
  font-size: 0.625rem;
  padding: 2px 6px;
  border: 1px solid rgb(var(--v-theme-accent));
  border-radius: 4px;
  color: rgb(var(--v-theme-accent));
}
.atlas-shell-nav__links {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-left: 1.5rem;
}
.atlas-shell-nav__link {
  padding: 18px 12px;
  color: rgb(var(--v-theme-primary));
  font-size: 14px;
  text-decoration: none;
  transition: color 0.15s ease-in-out;
}
.atlas-shell-nav__link:hover {
  color: rgb(var(--v-theme-accent));
}
.atlas-shell-nav__right {
  margin-left: auto;
  display: flex;
  align-items: center;
}
.atlas-shell-nav__api {
  font-size: 0.75rem;
  color: rgb(var(--v-theme-info));
  background: rgba(var(--v-theme-info), 0.08);
  padding: 4px 8px;
  border-radius: 4px;
}
@media (max-width: 959px) {
  .atlas-shell-nav__links { display: none; }
}
</style>`,
      "src/views/HomeView.vue": `<script setup lang="ts">
import { ref } from 'vue';

const items = ref([
  { id: 1, name: 'Example Cohort', author: 'developer', created: '2024-01-15' },
  { id: 2, name: 'Drug Exposure Analysis', author: 'developer', created: '2024-02-20' },
  { id: 3, name: 'Condition Occurrence', author: 'admin', created: '2024-03-10' },
]);

const headers = [
  { title: 'ID', key: 'id', width: '80px' },
  { title: 'Name', key: 'name' },
  { title: 'Author', key: 'author' },
  { title: 'Created', key: 'created' },
];
</script>

<template>
  <div class="page-wrapper">
    <div class="page-card">
      <v-container fluid class="pa-0">
        <h2 class="text-h5 font-weight-bold mb-2" style="color: rgb(var(--v-theme-primary))">
          Atlas Plugin
        </h2>
        <p class="text-body-1 mb-6" style="color: rgb(var(--v-theme-primary)); line-height: 1.6">
          This is your Atlas plugin. Edit this view or add new views to build your feature.
        </p>

        <div class="d-flex ga-4 mb-6">
          <v-btn prepend-icon="mdi-plus">New Item</v-btn>
          <v-btn variant="outlined" style="border-width: 2px; border-color: rgb(var(--v-theme-orange)); color: rgb(var(--v-theme-secondary))">
            Import
          </v-btn>
        </div>

        <v-text-field
          placeholder="Search..."
          prepend-inner-icon="mdi-magnify"
          variant="outlined"
          density="comfortable"
          class="mb-4"
          style="max-width: 400px"
          hide-details
        />

        <v-data-table
          :headers="headers"
          :items="items"
          density="comfortable"
          :elevation="0"
        >
          <template #no-data>
            <v-alert type="info" variant="tonal" class="ma-4">
              No data available
            </v-alert>
          </template>
        </v-data-table>
      </v-container>
    </div>
  </div>
</template>

<style scoped>
.page-wrapper {
  min-height: 100%;
  background: rgb(var(--v-theme-background));
  padding: 32px;
  display: flex;
  flex-direction: column;
}
.page-card {
  background: #fff;
  border-radius: 18px;
  padding: 30px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.08);
  flex: 1;
}
</style>`,
      "src/vite-env.d.ts": `/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_WEBAPI_URL: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}`,
    },
  },
  {
    id: "blank",
    name: "Blank",
    description: "Empty project with package.json",
    tech_stack: "node",
    dev_command: "npm run dev",
    install_command: "npm install",
    build_command: "npm run build",
    files: {
      "AI_RULES.md": `# Tech Stack
- This is a blank project. Set up the tech stack as needed.
- UPDATE the main entry point to include new code. OTHERWISE, the user can NOT see any changes!
`,
      "package.json": `{
  "name": "my-app",
  "version": "1.0.0",
  "private": true,
  "scripts": {
    "dev": "echo 'No dev script configured'",
    "build": "echo 'No build script configured'"
  }
}`,
    },
  },
];

/**
 * Scaffold a template into the given directory.
 */
export async function scaffoldTemplate(templateId: string, targetDir: string, appId?: string): Promise<void> {
  const template = TEMPLATES.find((t) => t.id === templateId);
  if (!template) {
    // Fall back to blank
    const blank = TEMPLATES.find((t) => t.id === "blank")!;
    for (const [filePath, content] of Object.entries(blank.files)) {
      await Deno.writeTextFile(`${targetDir}/${filePath}`, content);
    }
    return;
  }

  // Write inline files, replacing __APP_ID__ placeholder with actual app ID
  for (const [filePath, content] of Object.entries(template.files)) {
    const dir = filePath.includes("/") ? filePath.substring(0, filePath.lastIndexOf("/")) : null;
    if (dir) {
      await Deno.mkdir(`${targetDir}/${dir}`, { recursive: true });
    }
    const finalContent = appId ? content.replace(/__APP_ID__/g, appId) : content;
    await Deno.writeTextFile(`${targetDir}/${filePath}`, finalContent);
  }

  // Install dependencies in the background — don't block app creation
  // The dev server start will also check for node_modules and install if needed
  duckdb(`SELECT * FROM trex_devx_run_command('${escapeSql(targetDir)}', 'npm install')`)
    .catch((err) => console.warn("npm install during scaffold failed:", err.message));

  // Register D2E app functions with the trex plugin system
  if (template.tech_stack === "d2e-react" && appId) {
    registerAppFunctions(targetDir).catch((err) =>
      console.warn("Function registration failed:", err.message)
    );
  }
}

/**
 * Register an app's functions directory with the trex plugin system.
 */
async function registerAppFunctions(appDir: string): Promise<void> {
  const basePath = Deno.env.get("BASE_PATH") || "/trex";
  const res = await fetch(`http://localhost:8001${basePath}/api/plugins/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path: appDir }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Registration failed (${res.status}): ${body}`);
  }
  console.log(`[devx] Registered app functions from ${appDir}`);
}

/**
 * Inject the DevX component tagger Vite plugin into a scaffolded project.
 */
export async function injectComponentTagger(targetDir: string): Promise<void> {
  // Create .devx directory
  await Deno.mkdir(`${targetDir}/.devx`, { recursive: true });

  // Copy tagger plugin — try import.meta.url first, fall back to plugin mount path
  let taggerSource = "";
  const taggerCandidates = [
    new URL("./visual_editing/component_tagger_plugin.js", import.meta.url).pathname,
    "/usr/src/plugins-dev/devx/functions/visual_editing/component_tagger_plugin.js",
  ];
  for (const p of taggerCandidates) {
    try {
      taggerSource = await Deno.readTextFile(p);
      break;
    } catch {
      // try next
    }
  }
  if (!taggerSource) throw new Error("Could not load component_tagger_plugin.js");
  await Deno.writeTextFile(`${targetDir}/.devx/component_tagger_plugin.js`, taggerSource);

  // Find and patch vite.config
  for (const name of ["vite.config.ts", "vite.config.js", "vite.config.mts", "vite.config.mjs"]) {
    try {
      const configPath = `${targetDir}/${name}`;
      let config = await Deno.readTextFile(configPath);
      if (config.includes("devxComponentTagger")) return; // already patched

      // Add import
      config = `import devxComponentTagger from './.devx/component_tagger_plugin.js';\n` + config;

      // Add to plugins array
      config = config.replace(/plugins:\s*\[/, "plugins: [devxComponentTagger(), ");

      await Deno.writeTextFile(configPath, config);
      return;
    } catch {
      // config file doesn't exist, try next
    }
  }
}
