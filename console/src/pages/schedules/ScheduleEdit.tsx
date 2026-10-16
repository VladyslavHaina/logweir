// Guided create and the future-policy edit: one form, two routes.
import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { Alert, AlertActionLink, Breadcrumb, BreadcrumbItem, Card, CardBody, PageSection } from "@patternfly/react-core";
import LockIcon from "@patternfly/react-icons/dist/esm/icons/lock-icon";
import { ApiError } from "../../api/client";
import { useDestinations } from "../../api/queries";
import { mintIntent, useCreateSchedule, useEditPolicy, useSchedule, type Schedule } from "../../api/schedules";
import { PageHeader } from "../../components/PageHeader";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { createBody, draftValues, editBody, outcomeOf, placeFieldErrors, valuesOf } from "./model";
import { WriteAlert, useGrant } from "./parts";
import { ScheduleForm } from "./ScheduleForm";

function Crumbs({ ws, name, last }: { ws: string; name?: string; last: string }) {
  return (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ws/schedules" params={{ ws }}>Schedules</Link>} />
      {name && <BreadcrumbItem render={() => <Link to="/$ws/schedules/$name" params={{ ws, name }} search={{}}>{name}</Link>} />}
      <BreadcrumbItem isActive>{last}</BreadcrumbItem>
    </Breadcrumb>
  );
}

function ReadOnly({ ws, what }: { ws: string; what: string }) {
  return (
    <PageSection>
      <Card>
        <Empty title="Not available to this login" icon={LockIcon}>
          This login may read the schedules in {ws} and not {what}: that needs the operator or administrator role.
        </Empty>
      </Card>
    </PageSection>
  );
}

export function ScheduleCreate() {
  const { ws } = useParams({ from: "/$ws/schedules/new" });
  const navigate = useNavigate();
  const grant = useGrant(ws);
  const destinations = useDestinations(ws);
  const create = useCreateSchedule(ws);
  // One intent per draft: a double click, or a resend after no answer, is ONE schedule.
  const [intent, setIntent] = useState(() => mintIntent("schedule"));
  const header = (
    <PageHeader
      title="Create a schedule"
      breadcrumb={<Crumbs ws={ws} last="Create" />}
      description="What to back up, when, and where. The server names the schedule."
    />
  );
  if (grant && !grant.scheduleCreate) {
    return (
      <>
        {header}
        <ReadOnly ws={ws} what="create one" />
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
              ws={ws}
              initial={draftValues(destinations.data)}
              submitText="Create schedule"
              pending={create.isPending}
              error={create.error}
              onCancel={() => void navigate({ to: "/$ws/schedules", params: { ws } })}
              onSubmit={(values, line) =>
                create.mutate(
                  { body: createBody(values, line), intent },
                  {
                    onSuccess: (r) =>
                      void navigate({ to: "/$ws/schedules/$name", params: { ws, name: r.item.name }, search: { done: "created" } }),
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
  const { ws, name } = useParams({ from: "/$ws/schedules/$name/edit" });
  const navigate = useNavigate();
  const grant = useGrant(ws);
  const schedule = useSchedule(ws, name);
  const edit = useEditPolicy(ws, name);
  // DUI-13: the revision this form was opened on, held until "Read it again". A re-read
  // the page did not ask for (the write's invalidation after a 412, a refocus) must not
  // swap the form for a newer revision and drop what was typed.
  const [opened, setOpened] = useState<Schedule | null>(null);
  useEffect(() => {
    if (opened === null && schedule.data) setOpened(schedule.data);
  }, [opened, schedule.data]);
  const header = <PageHeader title={`Edit ${name}`} breadcrumb={<Crumbs ws={ws} name={name} last="Edit policy" />} />;
  if (grant && !grant.scheduleCreate) {
    return (
      <>
        {header}
        <ReadOnly ws={ws} what="change their policies" />
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
  const s = opened ?? schedule.data;
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
              ws={ws}
              saved={s}
              initial={valuesOf(s)}
              submitText="Save policy"
              pending={edit.isPending}
              error={edit.error}
              onCancel={() => void navigate({ to: "/$ws/schedules/$name", params: { ws, name }, search: {} })}
              onSubmit={(values, line) =>
                edit.mutate(editBody(values, line, generation), {
                  onSuccess: () => void navigate({ to: "/$ws/schedules/$name", params: { ws, name }, search: { done: "saved" } }),
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
                          // The revision in force now becomes the form's, once it has been read.
                          void schedule.refetch().then((read) => {
                            if (read.data) setOpened(read.data);
                          });
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
