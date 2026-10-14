import { defineConfig, mergeConfig } from "vitest/config";
import viteConfig from "./vite.config.ts";

// The tests run on jsdom with the mock transport (vite.config.ts picks "mock" when
// not building); a row that needs the live client builds its own with a fake fetch.
export default mergeConfig(
  viteConfig({ command: "serve", mode: "test" }),
  defineConfig({
    test: {
      environment: "jsdom",
      include: ["src/**/*.test.{ts,tsx}"],
      restoreMocks: true,
    },
  }),
);
