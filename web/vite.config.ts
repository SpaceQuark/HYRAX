import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  build: {
    // The control plane serves the dashboard from its own jar.
    outDir: "../server/src/main/resources/static",
    emptyOutDir: true,
  },
  server: {
    proxy: { "/api": "http://localhost:7070" },
  },
});
