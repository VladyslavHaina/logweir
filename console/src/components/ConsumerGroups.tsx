// DUI-25: the consumer groups whose positions a schedule's runs, or one run, record
// (`spec.consumerGroups`), or that none are: then no restore of them maps a position
// and Switch consumers over has nothing to move.

export function ConsumerGroups({ groups, each }: { groups: string[] | null | undefined; each: string }) {
  if (!groups?.length) {
    return <span className="lw-subtle">None: no consumer positions are recorded, so a restore cannot switch consumers over.</span>;
  }
  return (
    <>
      <ul className="lw-plain-list" aria-label="Consumer groups">
        {groups.map((g) => (
          <li key={g} className="lw-break lw-mono">
            {g}
          </li>
        ))}
      </ul>
      <div className="lw-subtle lw-small">{each}</div>
    </>
  );
}
