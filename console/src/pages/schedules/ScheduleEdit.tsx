// Guided create and the future-policy edit: one form, two routes.
import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { Alert, AlertActionLink, Breadcrumb, BreadcrumbItem, Card, CardBody, PageSection } from "@patternfly/react-core";
import LockIcon from "@patternfly/react-icons/dist/esm/icons/lock-icon";
import { ApiError } from "../../api/client";
import { mintIntent, useCreateSchedule, useDestinationChoices, useEditPolicy, useSchedule } from "../../api/schedules";
import { PageHeader } from "../../components/PageHeader";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { createBody, draftValues, editBody, outcomeOf, placeFieldErrors, valuesOf } from "./model";
import { WriteAlert, useGrant } from "./parts";
import { ScheduleForm } from "./ScheduleForm";

function Crumbs({ ns, name, last }: { ns: string; name?: string; last: string }) {
  return (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/schedules" params={{ ns }}>Schedules</Link>} />
      {name && <BreadcrumbItem render={() => <Link to="/$ns/schedules/$name" params={{ ns, name }} search={{}}>{name}</Link>} />}
      <BreadcrumbItem isActive>{last}</BreadcrumbItem>
    </Breadcrumb>
  );
}

function ReadOnly({ ns, what }: { ns: string; what: string }) {
  return (
    <PageSection>
      <Card>
        <Empty title="Not available to this login" icon={LockIcon}>
          This login may read the schedules in {ns} and not {what}: that needs the operator or administrator role.
        </Empty>
      </Card>
    </PageSection>
  );
}

export function ScheduleCreate() {
  const { ns } = useParams({ from: "/$ns/schedules/new" });
  const navigate = useNavigate();
  const grant = useGrant(ns);
  const destinations = useDestinationChoices(ns);
  const create = useCreateSchedule(ns);
  // One intent per draft: a double click, or a resend after no answer, is ONE schedule.
  const [intent, setIntent] = useState(() => mintIntent("schedule"));
  const header = (
    <PageHeader
      title="Create a schedule"
      breadcrumb={<Crumbs ns={ns} last="Create" />}
      description="What to back up, when, and where. The server names the schedule."
    />
  );
  if (grant && !grant.scheduleCreate) {
    return (
      <>
        {header}
        <ReadOnly ns={ns} what="create one" />
      </>
    );
  }
  if (destinations.isPending) {
    return (
      <>
        {header}
        <PageSection>
          <LoadingRows rows={6} label="Loading the form" />
        </PageSection>
      </>
    );
  }
  const placed = placeFieldErrors(create.error);
  const reused = create.error instanceof ApiError && create.error.code === "idempotency_conflict";
  return (
    <>
      {header}
      <PageSection>
        <Card>
          <CardBody>
            <ScheduleForm
              ns={ns}
              initial={draftValues(destinations.data)}
              submitText="Create schedule"
              pending={create.isPending}
              error={create.error}
              onCancel={() => void navigate({ to: "/$ns/schedules", params: { ns } })}
              onSubmit={(values, line) =>
                create.mutate(
                  { body: createBody(values, line), intent },
                  {
                    onSuccess: (r) =>
                      void navigate({ to: "/$ns/schedules/$name", params: { ns, name: r.item.name }, search: { done: "created" } }),
                  },
                )
              }
              status={
                create.isError && (
                  <WriteAlert
                    error={create.error}
                    kind="create"
                    subject="the schedule"
                    verb="created"
                    unmatched={placed.unmatched}
                    onRetry={
                      reused
                        ? () => {
                            setIntent(mintIntent("schedule"));
                            create.reset();
                          }
                        : undefined
                    }
                    retryText="Start a new request"
                  >
                    {reused && (
                      <p>
                        This draft's request key already made a schedule from different values, probably the earlier attempt
                        whose answer never arrived. Check the schedules list before starting a new request, or you will have two.
                      </p>
                    )}
                  </WriteAlert>
                )
              }
            />
          </CardBody>
        </Card>
      </PageSection>
    </>
  );
}

export function ScheduleEdit() {
  const { ns, name } = useParams({ from: "/$ns/schedules/$name/edit" });
  const navigate = useNavigate();
  const grant = useGrant(ns);
  const schedule = useSchedule(ns, name);
  const edit = useEditPolicy(ns, name);
  const header = <PageHeader title={`Edit ${name}`} breadcrumb={<Crumbs ns={ns} name={name} last="Edit policy" />} />;
  if (grant && !grant.scheduleCreate) {
    return (
      <>
        {header}
        <ReadOnly ns={ns} what="change their policies" />
      </>
    );
  }
  if (schedule.isPending) {
    return (
      <>
        {header}
        <PageSection>
          <LoadingRows rows={6} label="Loading the schedule" />
        </PageSection>
      </>
    );
  }
  if (schedule.isError) {
    return (
      <>
        {header}
        <PageSection>
          <ErrorState error={schedule.error} onRetry={() => void schedule.refetch()} />
        </PageSection>
      </>
    );
  }
  const s = schedule.data;
  const generation = s.generation;
  if (typeof generation !== "number") {
    // The route's precondition IS the revision; without one an edit would land on whatever is current.
    return (
      <>
        {header}
        <PageSection>
          <Alert isInline variant="warning" title="This schedule's revision is not published, so its policy cannot be edited here">
            The edit is refused rather than applied to a revision this page has not read. Reload the page; if it persists,
            the API serving this console is older than the console.
          </Alert>
        </PageSection>
      </>
    );
  }
  const placed = placeFieldErrors(edit.error);
  const outcome = edit.isError ? outcomeOf(edit.error) : null;
  return (
    <>
      {header}
      <PageSection>
        <Card>
          <CardBody>
            <ScheduleForm
              // A re-read revision is a different form: what was typed was typed against the old one.
              key={generation}
              ns={ns}
              saved={s}
              initial={valuesOf(s)}
              submitText="Save policy"
              pending={edit.isPending}
              error={edit.error}
              onCancel={() => void navigate({ to: "/$ns/schedules/$name", params: { ns, name }, search: {} })}
              onSubmit={(values, line) =>
                edit.mutate(editBody(values, line, generation), {
                  onSuccess: () => void navigate({ to: "/$ns/schedules/$name", params: { ns, name }, search: { done: "saved" } }),
                })
              }
              status={
                edit.isError &&
                (outcome === "precondition" ? (
                  <Alert
                    isInline
                    variant="warning"
                    title="Somebody changed this schedule after this page read it"
                    actionLinks={
                      <AlertActionLink
                        onClick={() => {
                          edit.reset();
                          void schedule.refetch();
                        }}
                      >
                        Read it again
                      </AlertActionLink>
                    }
                  >
                    Nothing was saved. Read the revision in force, then make your change again.
                  </Alert>
                ) : (
                  <WriteAlert error={edit.error} kind="replace" subject="the policy" verb="saved" unmatched={placed.unmatched} />
                ))
              }
            />
          </CardBody>
        </Card>
      </PageSection>
    </>
  );
}
