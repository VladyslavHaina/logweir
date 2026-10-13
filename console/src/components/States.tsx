// Loading, empty and error states: one look everywhere.
import type { ReactNode } from "react";
import {
  Button,
  EmptyState,
  EmptyStateActions,
  EmptyStateBody,
  EmptyStateFooter,
  Skeleton,
  Stack,
  StackItem,
} from "@patternfly/react-core";
import ExclamationCircleIcon from "@patternfly/react-icons/dist/esm/icons/exclamation-circle-icon";
import { ApiError } from "../api/client";

export function LoadingRows({ rows = 5, label = "Loading" }: { rows?: number; label?: string }) {
  return (
    <Stack hasGutter aria-busy="true" aria-label={label}>
      {Array.from({ length: rows }, (_, i) => (
        <StackItem key={i}>
          <Skeleton height="2rem" screenreaderText={i === 0 ? label : undefined} />
        </StackItem>
      ))}
    </Stack>
  );
}

export function ErrorState({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const api = error instanceof ApiError ? error : null;
  const headline =
    api?.status === 404
      ? "Not found"
      : api?.status === 403
        ? "You don't have access to this"
        : api && api.status >= 500
          ? "The Logweir service had a problem"
          : "Couldn't load this";
  return (
    <EmptyState titleText={headline} headingLevel="h2" icon={ExclamationCircleIcon} status="danger" variant="sm">
      <EmptyStateBody>
        {error instanceof Error ? error.message : String(error)}
        {api && (
          <span className="lw-error-meta">
            {api.status} {api.code}
            {api.requestId ? ` · request ${api.requestId}` : ""}
          </span>
        )}
      </EmptyStateBody>
      {onRetry && (
        <EmptyStateFooter>
          <EmptyStateActions>
            <Button variant="secondary" onClick={onRetry}>
              Try again
            </Button>
          </EmptyStateActions>
        </EmptyStateFooter>
      )}
    </EmptyState>
  );
}

export function Empty({
  title,
  children,
  icon,
  actions,
}: {
  title: string;
  children: ReactNode;
  icon?: React.ComponentType<any>;
  actions?: ReactNode;
}) {
  return (
    <EmptyState titleText={title} headingLevel="h2" icon={icon} variant="sm">
      <EmptyStateBody>{children}</EmptyStateBody>
      {actions && (
        <EmptyStateFooter>
          <EmptyStateActions>{actions}</EmptyStateActions>
        </EmptyStateFooter>
      )}
    </EmptyState>
  );
}
