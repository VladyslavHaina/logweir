// Pages the prototype names in the navigation but does not draw.
import { useParams } from "@tanstack/react-router";
import { Button, Card, PageSection } from "@patternfly/react-core";
import CubesIcon from "@patternfly/react-icons/dist/esm/icons/cubes-icon";
import { NAV } from "../app/Shell";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/States";

const PLANNED: Record<string, string> = {
  schedules: "Schedules: list, guided create with a cadence preview, suspend and resume, retention.",
  protection: "Protection policies: objective, newest point, alerts, and the schedules behind each.",
  approvals: "Approvals: requests waiting for you, the two-person Approve page, and the history.",
  catalog: "The recovery catalog: every point in the archive, its availability and its verification.",
  trust: "Trust: the keys the controller accepts, read-only.",
  connections: "Connections: add a Kafka cluster (the credential typed once), check it, discover topics.",
  destinations: "Destinations: saved buckets, access rotation, an access test, and what uses each.",
};

export function NotInPrototype() {
  const { ns, section } = useParams({ from: "/$ns/$section" });
  const entry = NAV.flatMap((g) => g.items).find((i) => i.path === section);
  return (
    <>
      <PageHeader title={entry?.label ?? section} />
      <PageSection>
        <Card>
          <Empty
            title="Not in the prototype"
            icon={entry?.icon ?? CubesIcon}
            actions={
              <Button component="a" href={`#/${ns}`} variant="secondary">
                Back to the overview
              </Button>
            }
          >
            {PLANNED[section] ?? "This page does not exist."} The prototype draws the overview, backups, the restore wizard,
            restores and the cutover review.
          </Empty>
        </Card>
      </PageSection>
    </>
  );
}
