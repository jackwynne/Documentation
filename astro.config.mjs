import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import tailwind from "@astrojs/tailwind";

import vercel from "@astrojs/vercel/serverless";

// https://astro.build/config
export default defineConfig({
  site: 'https://documentation-zeta-nine.vercel.app',
  integrations: [
    starlight({
      title: "Docs",
      social: {
        github: "https://github.com/jackwynne/documentation",
      },
      sidebar: [
        {
          label: "Power BI",
          autogenerate: {
            directory: "powerbi",
          },
        },
        // {
        // 	label: 'Guides',
        // 	items: [
        // 		// Each item here is one entry in the navigation menu.
        // 		{ label: 'Example Guide', link: '/guides/example/' },
        // 	],
        // },
        // {
        // 	label: 'Reference',
        // 	autogenerate: { directory: 'reference' },
        // },
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
