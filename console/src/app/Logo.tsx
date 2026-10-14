/** The Logweir mark (the same shape as the console's favicon) and word. */
export function Logo() {
  return (
    <span className="lw-logo">
      <svg viewBox="0 0 32 32" width="28" height="28" aria-hidden="true" focusable="false">
        <rect x="1" y="1" width="30" height="30" rx="8" className="lw-logo__tile" />
        <path className="lw-logo__weir" d="M6 8h20v3.2H6zM12 14.4h14v3.2H12zM18 20.8h8v3.2h-8z" />
      </svg>
      <span className="lw-logo__word">Logweir</span>
    </span>
  );
}
