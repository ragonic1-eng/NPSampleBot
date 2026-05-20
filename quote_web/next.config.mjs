/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // @react-pdf/renderer ships ESM-only. Without this flag, Next.js 14's
  // webpack pipeline refuses to bundle it and the production build fails
  // with "Module not found: ESM packages". 'loose' lets ESM packages be
  // referenced from CommonJS contexts — required for @react-pdf in app
  // router. See https://nextjs.org/docs/messages/import-esm-externals
  experimental: {
    esmExternals: "loose",
  },
};

export default nextConfig;
