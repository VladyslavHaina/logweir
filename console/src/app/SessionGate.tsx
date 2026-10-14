// The session gate: GET /api/v1/session once at boot, then one of four pages
// (console/README.md, "How the page decides its state"):
//   a session document          -> the console, for the namespaces it grants;
//   401 unauthenticated/expired -> "Sign in", handing over to /auth/login and back to this route;
//   anything else               -> "Can't reach the Logweir service", with what it got and Retry;
//   a session with no namespace -> what the actor holds (nothing) and Sign out.
// Once the console is up it stays up: a session that ends later is SessionClock's
// dialog over the page, never a sign-in page in its place (DUI-4).
import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { Bullseye, Button, EmptyState, EmptyStateActions, EmptyStateBody, EmptyStateFooter, Spinner } from "@patternfly/react-core";
import ExclamationCircleIcon from "@patternfly/react-icons/dist/esm/icons/exclamation-circle-icon";
import LockIcon from "@patternfly/react-icons/dist/esm/icons/lock-icon";
import UserIcon from "@patternfly/react-icons/dist/esm/icons/user-icon";
import { ApiError, readSession, setCsrfToken, signOut } from "../api/client";
import { keys } from "../api/queries";
import { SessionProvider } from "../api/session";
import type { Session } from "../api/types";
import { Logo } from "./Logo";
import { ErrorText } from "../components/Prose";
import { SessionClock, useSignInHref } from "./SessionClock";
import { shortSubject } from "./subject";

export function useSessionQuery() {
  return useQuery({
    queryKey: keys.session,
    queryFn: async () => {
      const session = await readSession();
      // In memory for the life of the page; every unsafe request takes it from there.
      setCsrfToken(session.csrfToken);
      return session;
    },
    staleTime: Infinity,
    retry: false,
    refetchOnWindowFocus: false,
  });
}

function FullPage({ children }: { children: ReactNode }) {
  return (
    <main className="lw-gate" id="lw-main" tabIndex={-1}>
      <div className="lw-gate__brand">
        <Logo />
      </div>
      <Bullseye>{children}</Bullseye>
    </main>
  );
}

export function SignedOut({ code }: { code: string }) {
  const ended = code === "session_expired";
  const href = useSignInHref();
  return (
    <FullPage>
      <EmptyState titleText={ended ? "Your session has ended" : "Sign in to Logweir"} headingLevel="h1" icon={LockIcon} variant="sm">
        <EmptyStateBody>
          {ended
            ? "Sign in again to carry on. You come back to the page you were on."
            : "This console uses your organisation's single sign-on. You come back to the page you asked for."}
        </EmptyStateBody>
        <EmptyStateFooter>
          <EmptyStateActions>
            <Button component="a" href={href} variant="primary">
              Sign in
            </Button>
          </EmptyStateActions>
        </EmptyStateFooter>
      </EmptyState>
    </FullPage>
  );
}

export function Unavailable({ error, onRetry }: { error: unknown; onRetry: () => void }) {
  const api = error instanceof ApiError ? error : null;
  return (
    <FullPage>
      <EmptyState titleText="Can't reach the Logweir service" headingLevel="h1" icon={ExclamationCircleIcon} status="danger" variant="sm">
        <EmptyStateBody>
          <ErrorText error={error} />
          {api && (
            <span className="lw-error-meta">
              {api.status || "no answer"} {api.code}
              {api.requestId ? ` · request ${api.requestId}` : ""}
            </span>
          )}
        </EmptyStateBody>
        <EmptyStateFooter>
          <EmptyStateActions>
            <Button variant="primary" onClick={onRetry}>
              Try again
            </Button>
          </EmptyStateActions>
        </EmptyStateFooter>
      </EmptyState>
    </FullPage>
  );
}

export function NoNamespace({ session }: { session: Session }) {
  const subject = shortSubject(session.actor.subject);
  const name = session.actor.displayName || subject;
  return (
    <FullPage>
      <EmptyState titleText="No namespace is granted to you" headingLevel="h1" icon={UserIcon} variant="sm">
        <EmptyStateBody>
          You are signed in as <strong className="lw-break">{name}</strong>
          {name !== subject && (
            <span className="lw-break" title={session.actor.subject}>
              {" "}
              ({subject})
            </span>
          )}
          , but your groups hold no Logweir role in any namespace. An administrator grants roles in the console's role bindings.
        </EmptyStateBody>
        {session.authenticationMode === "oidc" && (
          <EmptyStateFooter>
            <EmptyStateActions>
              <Button variant="secondary" onClick={() => void signOut().finally(() => window.location.reload())}>
                Sign out
              </Button>
            </EmptyStateActions>
          </EmptyStateFooter>
        )}
      </EmptyState>
    </FullPage>
  );
}

export function SessionGate({ children }: { children: (session: Session) => ReactNode }) {
  const query = useSessionQuery();
  const session = query.data;
  if (session === undefined) {
    if (query.isError) {
      const error = query.error;
      if (error instanceof ApiError && error.signedOut) return <SignedOut code={error.code} />;
      return <Unavailable error={error} onRetry={() => void query.refetch()} />;
    }
    return (
      <FullPage>
        <Spinner aria-label="Connecting to the Logweir service" />
      </FullPage>
    );
  }
  if (session.namespaces.length === 0) return <NoNamespace session={session} />;
  // A later read that failed keeps the document it had: the page stays, and the clock
  // says whether the service answered "sign in".
  const ended = query.isError && query.error instanceof ApiError && query.error.signedOut;
  return (
    <SessionProvider session={session}>
      {children(session)}
      <SessionClock session={session} ended={ended} recheck={() => query.refetch()} />
    </SessionProvider>
  );
}
