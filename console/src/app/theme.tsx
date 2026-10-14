// Light and dark: follow the system by default; the masthead switch overrides it
// for the life of the page. Nothing is written to browser storage.
import { createContext, useContext, useEffect, useState, type ReactNode } from "react";

export type ThemeChoice = "system" | "light" | "dark";

const DARK_CLASS = "pf-v6-theme-dark";

function systemDark(): boolean {
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ?? false;
}

function initialChoice(): ThemeChoice {
  // A link may ask for a theme (?theme=dark before the #), for screenshots and demos.
  const asked = new URLSearchParams(window.location.search).get("theme");
  return asked === "dark" || asked === "light" ? asked : "system";
}

const ThemeContext = createContext<{ choice: ThemeChoice; setChoice: (c: ThemeChoice) => void }>({
  choice: "system",
  setChoice: () => undefined,
});

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [choice, setChoice] = useState<ThemeChoice>(initialChoice);
  const [system, setSystem] = useState(systemDark);

  useEffect(() => {
    const query = window.matchMedia?.("(prefers-color-scheme: dark)");
    if (!query) return undefined;
    const onChange = () => setSystem(query.matches);
    query.addEventListener("change", onChange);
    return () => query.removeEventListener("change", onChange);
  }, []);

  const dark = choice === "dark" || (choice === "system" && system);
  useEffect(() => {
    document.documentElement.classList.toggle(DARK_CLASS, dark);
  }, [dark]);

  return <ThemeContext.Provider value={{ choice, setChoice }}>{children}</ThemeContext.Provider>;
}

export function useTheme() {
  return useContext(ThemeContext);
}
