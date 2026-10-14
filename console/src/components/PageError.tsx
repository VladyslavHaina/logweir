// DUI-16: one page that cannot render must not blank the console. The router gives
// every route this boundary (`defaultErrorComponent`), so an error in a page shows
// here, in the page's place, while the masthead and the navigation stay usable.
import type { ErrorComponentProps } from "@tanstack/react-router";
import { Button, Card, EmptyState, EmptyStateActions, EmptyStateBody, EmptyStateFooter, PageSection } from "@patternfly/react-core";
import ExclamationCircleIcon from "@patternfly/react-icons/dist/esm/icons/exclamation-circle-icon";
import { ErrorText } from "./Prose";

export function PageError({ error, reset }: ErrorComponentProps) {
  return (
    <PageSection>
      <Card>
        <EmptyState titleText="This page could not be shown" headingLevel="h1" icon={ExclamationCircleIcon} status="danger" variant="sm">
          <EmptyStateBody>
            <p>
              Something on it could not be displayed. Nothing was changed; the rest of the console still works, so use the navigation, or try
              this page again.
            </p>
            <p className="lw-subtle lw-small lw-gap-top">
              <ErrorText error={error} />
            </p>
          </EmptyStateBody>
          <EmptyStateFooter>
            <EmptyStateActions>
              <Button variant="primary" onClick={reset}>
                Try again
              </Button>
            </EmptyStateActions>
          </EmptyStateFooter>
        </EmptyState>
      </Card>
    </PageSection>
  );
}
