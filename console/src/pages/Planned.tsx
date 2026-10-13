// A page the navigation names but this build does not draw. In the mock builds it is a
// screen the prototype left out; in a live build it is a page whose rebuild run
// (R2-R7) has not landed yet, so it points at the same page in the current console.
import { useParams, useRouterState } from "@tanstack/react-router";
import { Button, Card, PageSection } from "@patternfly/react-core";
import CubesIcon from "@patternfly/react-icons/dist/esm/icons/cubes-icon";
import { MOCK } from "../api/client";
import { NAV } from "../app/Shell";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/States";

/** The current console's address for the same page (ui/README.md "Routes"). */
export function currentConsoleHref(ns: string, segments: string[]): string {
  const [section = "", name] = segments;
  const q = `ns=${encodeURIComponent(ns)}`;
  const one = (route: string) => `/ui/#/${route}?${q}${name ? `&name=${encodeURIComponent(name)}` : ""}`;
  switch (section) {
    case "restores":
      return name === "new" ? `/ui/#/restore?${q}` : one("history");
    default:
      return `/ui/#/clusters?${q}`;
  }
}

export function Planned() {
  const { ns = "" } = useParams({ strict: false });
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  const segments = pathname.split("/").filter(Boolean).slice(1);
  const section = segments[0] ?? "";
  const entry = NAV.flatMap((g) => g.items).find((i) => i.path === section);
  const known = Boolean(entry);
  return (
    <>
      <PageHeader title={entry?.label ?? section} />
      <PageSection>
        <Card>
          {MOCK ? (
            <Empty
              title="This page does not exist"
              icon={entry?.icon ?? CubesIcon}
              actions={
                <Button component="a" href={`#/${ns}`} variant="secondary">
                  Back to the overview
                </Button>
              }
            >
              Nothing in the console has this address. The prototype draws every page in the navigation.
            </Empty>
          ) : (
            <Empty
              title={known ? "Not in the new console yet" : "This page does not exist"}
              icon={entry?.icon ?? CubesIcon}
              actions={
                known ? (
                  <Button component="a" href={currentConsoleHref(ns, segments)} variant="primary">
                    Open it in the current console
                  </Button>
                ) : (
                  <Button component="a" href={`#/${ns}`} variant="secondary">
                    Back to the overview
                  </Button>
                )
              }
            >
              {known
                ? "This preview of the rebuilt console does not have this page yet. It is still in the current console, with the same session."
                : "Nothing in the console has this address."}
            </Empty>
          )}
        </Card>
      </PageSection>
    </>
  );
}
