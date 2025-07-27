import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import tailwind from "@astrojs/tailwind";

import vercel from "@astrojs/vercel/serverless";

// https://astro.build/config
export default defineConfig({
  site: "https://documentation-zeta-nine.vercel.app",
  integrations: [
    starlight({
      title: "Docs",
      editLink: {
        baseUrl: "https://github.com/jackwynne/documentation/edit/main/",
      },
      lastUpdated: true,
      social: [
        { icon: 'github', label: 'GitHub', href: "https://github.com/jackwynne/documentation"},
      ],
      sidebar: [
        {
          label: "Power BI",
          autogenerate: {
            directory: "powerbi",
          },
        },
        {
          label: "Azure",
          autogenerate: {
            directory: "azure",
          },
        },
        {
          label: "Windows",
          autogenerate: {
            directory: "windows",
          },
        },
        {
          label: "Spark",
          autogenerate: {
            directory: "spark",
          },
        },
      ],

      customCss: ["./src/tailwind.css"],
    }),
    tailwind({
      applyBaseStyles: false,
    }),
  ],
  // output: "server",
  // adapter: vercel(),
});
