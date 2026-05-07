import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

const config: Config = {
  title: "trexsql",
  tagline: "Distributed SQL Engine",
  favicon: "img/favicon.ico",

  url: "http://localhost:8001",
  baseUrl: "/plugins/trex/docs/",

  markdown: {
    mermaid: true,
  },
  themes: ["@docusaurus/theme-mermaid"],

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      {
        docs: {
          routeBasePath: "/",
          sidebarPath: "./sidebars.ts",
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    [
      "@docusaurus/plugin-client-redirects",
      {
        redirects: [
          // Section rename: js-interface/* → apis/*
          { from: "/js-interface/graphql", to: "/apis/graphql" },
          { from: "/js-interface/auth", to: "/apis/auth" },
          { from: "/js-interface/mcp", to: "/apis/mcp" },
          { from: "/js-interface/functions", to: "/apis/functions" },
          // build-a-plugin promoted from quickstart to tutorial
          { from: "/quickstarts/build-a-plugin", to: "/tutorials/build-a-plugin" },
        ],
      },
    ],
  ],

  themeConfig: {
    navbar: {
      title: "trexsql",
      items: [
        {
          type: "docSidebar",
          sidebarId: "docsSidebar",
          position: "left",
          label: "Documentation",
        },
      ],
    },
    footer: {
      style: "dark",
      copyright: `Copyright © ${new Date().getFullYear()} trexsql`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ["sql", "bash", "json", "toml"],
    },
    mermaid: {
      theme: { light: "base", dark: "base" },
      options: {
        themeVariables: {
          // Light-mode palette — slate text on soft blue/violet fills.
          primaryColor: "#eef2ff",
          primaryBorderColor: "#6366f1",
          primaryTextColor: "#1e1b4b",
          secondaryColor: "#f0f9ff",
          secondaryBorderColor: "#0ea5e9",
          secondaryTextColor: "#0c4a6e",
          tertiaryColor: "#f5f3ff",
          tertiaryBorderColor: "#8b5cf6",
          tertiaryTextColor: "#4c1d95",
          // Subgraph + cluster styling
          clusterBkg: "#f8fafc",
          clusterBorder: "#cbd5e1",
          // Default node fallback
          nodeBorder: "#94a3b8",
          // Edges / arrows
          lineColor: "#475569",
          edgeLabelBackground: "#ffffff",
          // Sequence diagrams
          actorBkg: "#eef2ff",
          actorBorder: "#6366f1",
          actorTextColor: "#1e1b4b",
          actorLineColor: "#94a3b8",
          // Notes
          noteBkgColor: "#fef9c3",
          noteBorderColor: "#eab308",
          noteTextColor: "#713f12",
          fontFamily:
            'system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
          fontSize: "14px",
        },
      },
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
