import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./lib/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        // NP Foods brand orange — used for the table header band + accents.
        npOrange: "#E25822",
      },
    },
  },
  plugins: [],
};

export default config;
