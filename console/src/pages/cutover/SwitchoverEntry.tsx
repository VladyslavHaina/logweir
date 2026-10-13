// For R5's restore page: the "Consumer positions" card. It says whether this restore
// can be switched over (the restore's own facts, §3.4; the review decides the rest),
// lists the Switchovers that name it, and offers "Switch consumers over".
import { Link, useNavigate } from "@tanstack/react-router";
import { Button, Card, CardBody, CardHeader, CardTitle, Content } from "@patternfly/react-core";
import ExchangeAltIcon from "@patternfly/react-icons/dist/esm/icons/exchange-alt-icon";
import { useSwitchovers, type Restore } from "../../api/switchovers";
import { cutoverRefusal } from "./Cutover";
import { groupSummary } from "./SwitchoverList";
import { PhaseLabel } from "./status";

export function SwitchoverEntry({ restore: r, canCreate = true }: { restore: Restore; canCreate?: boolean }) {
  const navigate = useNavigate();
  const list = useSwitchovers(r.namespace);
  const refusal = cutoverRefusal(r);
  const mine = (list.data ?? []).filter((s) => s.restore.name === r.name && s.restore.uid === r.uid);
  return (
    <Card>
      <CardHeader
        actions={
          !refusal && canCreate
            ? {
                actions: (
                  <Button variant="secondary" icon={<ExchangeAltIcon />} onClick={() => void navigate({ to: "/$ns/restores/$name/cutover", params: { ns: r.namespace, name: r.name }, search: {} })}>
                    Switch consumers over
                  </Button>
                ),
                hasNoOffset: true,
              }
            : undefined
        }
      >
        <CardTitle component="h2">Consumer positions</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className={refusal ? "lw-subtle" : undefined}>
          {refusal
            ? `${refusal.body}`
            : "The complete verification mapped each captured consumer group's position onto the restored topics. Review them and move the stopped groups here."}
          {refusal && <span className="lw-reason"> {refusal.word}</span>}
        </Content>
        {mine.length > 0 && (
          <ul className="lw-plain-list lw-gap-top" aria-label="Switchovers of this restore">
            {mine.map((s) => (
              <li key={s.uid || s.name}>
                <Link to="/$ns/switchovers/$name" params={{ ns: r.namespace, name: s.name }}>
                  {s.name}
                </Link>{" "}
                <PhaseLabel sw={s} /> <span className="lw-subtle lw-small">{groupSummary(s)}</span>
              </li>
            ))}
          </ul>
        )}
      </CardBody>
    </Card>
  );
}
