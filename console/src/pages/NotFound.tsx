// An address under a namespace that names no page: what it is, and the way back.
import { useParams, useRouterState } from "@tanstack/react-router";
import { Button, Card, PageSection } from "@patternfly/react-core";
import CubesIcon from "@patternfly/react-icons/dist/esm/icons/cubes-icon";
import { PageHeader } from "../components/PageHeader";
import { Empty } from "../components/States";

export function NotFound() {
  const { ns = "" } = useParams({ strict: false });
  const section = useRouterState({ select: (s) => s.location.pathname.split("/").filter(Boolean)[1] ?? "" });
  return (
    <>
      <PageHeader title={section} />
      <PageSection>
        <Card>
          <Empty
            title="This page does not exist"
            icon={CubesIcon}
            actions={
              <Button component="a" href={`#/${ns}`} variant="secondary">
                Back to the overview
              </Button>
            }
          >
            Nothing in the console has this address.
          </Empty>
        </Card>
      </PageSection>
    </>
  );
}
