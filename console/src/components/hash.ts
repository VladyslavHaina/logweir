// The sha256 of a plan, as the page shows it before anything is created.
import { useEffect, useState } from "react";

export async function sha256(text: string): Promise<string> {
  if (!globalThis.crypto?.subtle) return "unavailable: this page is not in a secure context";
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return "sha256:" + Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

export function useSha256(text: string): string {
  const [hash, setHash] = useState("computing...");
  useEffect(() => {
    let live = true;
    void sha256(text).then((h) => live && setHash(h));
    return () => {
      live = false;
    };
  }, [text]);
  return hash;
}
