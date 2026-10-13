import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

// Node's, read at build time; the console's own code has no `process` (no @types/node).
declare const process: { env: Record<string, string | undefined> };

// Two builds from one source:
//   `vite build`               -> dist/: index.html plus hashed files under assets/,
//                                 what logweir-api serves at /ui/. It talks to the
//                                 product API on its own origin ("live").
//   `vite build --mode single` -> dist-single/index.html: every script, style and
//                                 font inlined, on the in-page mock, for showing the
//                                 design as one page.
// `vite dev` and the tests run on the mock unless VITE_LOGWEIR_API=live is set; with
// it, `vite dev` proxies /api and /auth to a logweir-api on 127.0.0.1:8484.
export default defineConfig(({ command, mode }) => {
  const single = mode === "single";
  const api = single ? "mock" : (process.env.VITE_LOGWEIR_API ?? (command === "build" ? "live" : "mock"));
  if (api !== "live" && api !== "mock") throw new Error(`VITE_LOGWEIR_API is "live" or "mock", not "${api}"`);
  return {
    base: "./",
    define: { "import.meta.env.VITE_LOGWEIR_API": JSON.stringify(api) },
    plugins: single ? [react(), inlineIntoHtml()] : [react(), pageIcon()],
    server: {
      // The API checks Host and, on writes, Origin against its own public origin.
      proxy: Object.fromEntries(
        ["/api", "/auth"].map((path) => [
          path,
          { target: "http://127.0.0.1:8484", changeOrigin: true, headers: { origin: "http://127.0.0.1:8484" } },
        ]),
      ),
    },
    build: single
      ? {
          outDir: "dist-single",
          assetsInlineLimit: () => true,
          cssCodeSplit: false,
          modulePreload: { polyfill: false },
          reportCompressedSize: false,
        }
      : {
          outDir: "dist",
          assetsDir: "assets",
          sourcemap: false,
          modulePreload: { polyfill: false },
          // The licences of every package bundled into the page, for the image's
          // /usr/share/licenses/logweir/ (Dockerfile.console); never served.
          license: true,
          // The mock's modules only define data. Saying so lets a live build drop them
          // whole once nothing reads them (scripts/check-dist.mjs holds it to that).
          rolldownOptions: { treeshake: { moduleSideEffects: [{ test: /\/src\/mock\//, sideEffects: false }] } },
        },
  };
});

// The page icon logweir-api compiles in and serves beside the page (assets.rs ICON_PATH).
// The single-file demo has no server, so it names none.
function pageIcon(): Plugin {
  return {
    name: "logweir-page-icon",
    transformIndexHtml: () => [{ tag: "link", attrs: { rel: "icon", type: "image/svg+xml", href: "./favicon.svg" }, injectTo: "head" }],
  };
}

// The single-file build's one step: move the entry chunk and the stylesheet into
// index.html and drop them from the output. Our own 30 lines instead of a plugin
// dependency (vite-plugin-singlefile pulls micromatch/braces, which carry an
// unpatched advisory as of 2026-10-13).
function inlineIntoHtml(): Plugin {
  return {
    name: "logweir-inline-into-html",
    enforce: "post",
    generateBundle(_options, bundle) {
      const page = bundle["index.html"];
      if (!page || page.type !== "asset") {
        this.error("no index.html in the bundle");
      }
      let html = String(page.source);
      for (const [fileName, file] of Object.entries(bundle)) {
        if (file.type === "chunk") {
          const tag = new RegExp(`<script[^>]*src="[^"]*${escapeRe(fileName)}"[^>]*></script>`);
          if (!tag.test(html)) this.error(`no script tag for ${fileName}`);
          const code = file.code.replace(/<\/script/gi, "<\\/script");
          html = html.replace(tag, () => `<script type="module">${code}</script>`);
          delete bundle[fileName];
        } else if (fileName.endsWith(".css")) {
          const tag = new RegExp(`<link[^>]*href="[^"]*${escapeRe(fileName)}"[^>]*>`);
          if (!tag.test(html)) this.error(`no link tag for ${fileName}`);
          const css = String(file.source).replace(/<\/style/gi, "<\\/style");
          html = html.replace(tag, () => `<style>${css}</style>`);
          delete bundle[fileName];
        }
      }
      page.source = html;
    },
  };
}

function escapeRe(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
