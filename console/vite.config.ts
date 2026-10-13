import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

// Two builds from one source:
//   `vite build`               -> dist/: index.html plus hashed files under assets/,
//                                 the shape logweir-api would serve under /ui/.
//   `vite build --mode single` -> dist-single/index.html: every script, style and
//                                 font inlined, for showing the prototype as one page.
export default defineConfig(({ mode }) => {
  const single = mode === "single";
  return {
    base: "./",
    plugins: single ? [react(), inlineIntoHtml()] : [react()],
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
        },
  };
});

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
