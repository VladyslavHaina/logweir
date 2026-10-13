// An empty namespace: the four steps from nothing to a proven restore.
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Content,
  Flex,
  FlexItem,
  PageSection,
  ProgressStep,
  ProgressStepper,
} from "@patternfly/react-core";

const STEPS = [
  {
    id: "connect",
    title: "Connect a Kafka cluster",
    body: "Tell Logweir where your brokers are and which credential to use. The credential is stored as a Kubernetes Secret, never in the page.",
    action: "Add a connection",
    to: "connections",
  },
  {
    id: "destination",
    title: "Choose where backups go",
    body: "Save an S3-compatible bucket once; schedules and restores refer to it by name.",
    action: "Add a destination",
    to: "destinations",
  },
  {
    id: "schedule",
    title: "Schedule backups",
    body: "Pick topics and a cadence. Every run is signed and verified by the controller.",
    action: "Create a schedule",
    to: "schedules",
  },
  {
    id: "drill",
    title: "Prove a restore",
    body: "Restore a recovery point into new topics on a test cluster and let Logweir verify it.",
    action: "Start a restore",
    to: "restores/new",
  },
];

export function GettingStarted({ ns }: { ns: string }) {
  return (
    <PageSection>
      <Card>
        <CardHeader>
          <CardTitle component="h2">Get {ns} protected</CardTitle>
        </CardHeader>
        <CardBody>
          <Content component="p" className="lw-subtle">
            Nothing is set up in this namespace yet. These four steps take you from no backups to a restore you have
            seen work. Each one opens the page that does it.
          </Content>
          <ProgressStepper isVertical aria-label="Getting started" className="lw-getting-started">
            {STEPS.map((step, i) => (
              <ProgressStep
                key={step.id}
                id={`gs-${step.id}`}
                titleId={`gs-${step.id}-title`}
                variant={i === 0 ? "info" : "pending"}
                isCurrent={i === 0}
                description={
                  <Flex direction={{ default: "column" }} gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsFlexStart" }}>
                    <FlexItem>{step.body}</FlexItem>
                    <FlexItem>
                      <Button
                        component="a"
                        href={`#/${ns}/${step.to}`}
                        variant={i === 0 ? "primary" : "secondary"}
                        size="sm"
                        isDisabled={i !== 0}
                      >
                        {step.action}
                      </Button>
                    </FlexItem>
                  </Flex>
                }
              >
                {step.title}
              </ProgressStep>
            ))}
          </ProgressStepper>
        </CardBody>
      </Card>
    </PageSection>
  );
}
