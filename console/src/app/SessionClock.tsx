// DUI-4: the session's end, without losing the page. The session is capped
// (`sessionMaxAgeSeconds`, a security setting kept as it is), and GET
// /api/v1/session says when it ends (`expiresAt`). About two minutes before, a
// warning offers "Sign in again in a new tab": the sign-in round trip in another
// tab renews the cookie this tab shares, so nothing here is reloaded and no
// browser storage is needed. Once the service says the session has ended, a
// blocking dialog keeps this page, and every form on it, mounted until the user
// has signed in elsewhere and continues here.
//
// DUI-34: the warning is an inline alert at the top of the page content, under the
// masthead (SessionWarning, which the shell renders), never a toast over the page:
// a toast at the top right covered the page header's primary action.
import { createContext, useContext, useEffect, useRef, useState, useSyncExternalStore, type ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  Alert,
  AlertActionCloseButton,
  Button,
  Content,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  PageSection,
} from "@patternfly/react-core";
import { signInHref } from "../api/client";
import type { Session } from "../api/types";
import { shortSubject } from "./subject";

/** How long before the session's end the warning shows. */
export const WARN_BEFORE_MS = 120_000;
/** Past the end by this page's clock, how often the service is asked whether it agrees. */
export const RECHECK_MS = 15_000;

function subscribeLocation(onChange: () => void): () => void {
  window.addEventListener("hashchange", onChange);
  window.addEventListener("popstate", onChange);
  return () => {
    window.removeEventListener("hashchange", onChange);
    window.removeEventListener("popstate", onChange);
  };
}

/**
 * The sign-in link for the address shown NOW. Read at render only, a link drawn on one
 * page kept that page's `next` after the address changed in the same tab (DUI-9).
 */
export function useSignInHref(): string {
  return useSyncExternalStore(subscribeLocation, () => signInHref(window.location));
}

/** "1 min 40 s", "45 s": what is left, in words. */
export function leftWords(ms: number): string {
  const s = Math.max(0, Math.ceil(ms / 1000));
  const m = Math.floor(s / 60);
  return m > 0 ? `${m} min${s % 60 ? ` ${s % 60} s` : ""}` : `${s} s`;
}

/** The current time, every second. */
function useSecondTick(): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  return now;
}

/** A link that opens the sign-in round trip in a new tab: this tab is never navigated away. */
/**
 * DUI-41.7: a sign-in tab this page opened. A second click started a second sign-in, which
 * replaced the first, so the first tab ended on "the sign-in state does not match". Until the
 * person comes back to this tab, the link is held.
 */
const SignInContext = createContext<{ pending: boolean; opened: () => void }>({ pending: false, opened: () => undefined });

function SignInNewTab({ variant }: { variant: "primary" | "link" }) {
  const href = useSignInHref();
  const { pending, opened } = useContext(SignInContext);
  if (pending) {
    return (
      <Button variant={variant} isInline={variant === "link"} isAriaDisabled>
        Sign-in open in another tab
      </Button>
    );
  }
  return (
    <Button component="a" href={href} target="_blank" rel="noopener noreferrer" variant={variant} isInline={variant === "link"} onClick={opened}>
      Sign in again in a new tab
    </Button>
  );
}

/** The warning the page shows before the session's end: what is left, and its close. */
type Warning = { remaining: number; dismiss: () => void };

const WarningContext = createContext<Warning | null>(null);

/**
 * The session warning, inline at the top of the page content (DUI-34): it pushes the
 * page down rather than covering any of it. The region stays mounted, so a screen
 * reader hears the warning when it appears; the countdown in its title is not read
 * out every second.
 */
export function SessionWarning() {
  const warning = useContext(WarningContext);
  return (
    <div className="lw-session-warning" aria-live="polite">
      {warning && (
        <PageSection className="lw-session-warning__section">
          <Alert
            isInline
            variant="warning"
            title={<span aria-live="off">{warning.remaining > 0 ? `Your session ends in ${leftWords(warning.remaining)}` : "Your session is ending"}</span>}
            actionClose={<AlertActionCloseButton aria-label="Close the session warning" onClose={warning.dismiss} />}
            actionLinks={<SignInNewTab variant="link" />}
          >
            Signing in again in a new tab renews it for this tab too, and this page keeps what you typed.
          </Alert>
        </PageSection>
      )}
    </div>
  );
}

export function SessionClock({
  session,
  ended,
  recheck,
  children,
}: {
  session: Session;
  /** The service answered the session read with "sign in". */
  ended: boolean;
  /** Read the session again. */
  recheck: () => Promise<unknown>;
  /** The page: it stays mounted under the dialogs, and reads the warning from here. */
  children?: ReactNode;
}) {
  const client = useQueryClient();
  const now = useSecondTick();
  const reread = useRef(recheck);
  reread.current = recheck;
  const expires = session.expiresAt ? Date.parse(session.expiresAt) : NaN;
  const remaining = Number.isFinite(expires) ? expires - now : Infinity;
  const [dismissed, setDismissed] = useState<number | null>(null);
  const [asked, setAsked] = useState(false);
  const [signingIn, setSigningIn] = useState(false);
  // Back in this tab: the other tab's sign-in is done or abandoned, so the link is offered again.
  useEffect(() => {
    if (!signingIn) return undefined;
    const back = () => {
      if (document.visibilityState === "visible") setSigningIn(false);
    };
    window.addEventListener("focus", back);
    document.addEventListener("visibilitychange", back);
    return () => {
      window.removeEventListener("focus", back);
      document.removeEventListener("visibilitychange", back);
    };
  }, [signingIn]);
  const firstActor = useRef({ id: session.actor.id, name: session.actor.displayName || shortSubject(session.actor.subject) });
  const otherActor = session.actor.id !== firstActor.current.id;

  // Past the end by this page's clock: the service decides. Ask now, and again every
  // 15 s while it still answers (a clock that runs ahead of the service's).
  const due = remaining <= 0 && !ended;
  useEffect(() => {
    if (!due) return undefined;
    void reread.current();
    const timer = setInterval(() => void reread.current(), RECHECK_MS);
    return () => clearInterval(timer);
  }, [due]);

  // Back in this tab after signing in in another tab: read the session again.
  const near = ended || remaining <= WARN_BEFORE_MS;
  useEffect(() => {
    if (!near) return undefined;
    const back = () => {
      if (document.visibilityState === "visible") void reread.current();
    };
    window.addEventListener("focus", back);
    document.addEventListener("visibilitychange", back);
    return () => {
      window.removeEventListener("focus", back);
      document.removeEventListener("visibilitychange", back);
    };
  }, [near]);

  // Signed in again: the reads that were refused while signed out are read again.
  const wasEnded = useRef(ended);
  useEffect(() => {
    if (wasEnded.current && !ended) {
      setAsked(false);
      void client.invalidateQueries({ predicate: (q) => q.queryKey[0] !== "session" && q.state.status === "error" });
    }
    wasEnded.current = ended;
  }, [ended, client]);

  const warning: Warning | null =
    !otherActor && !ended && remaining <= WARN_BEFORE_MS && dismissed !== expires ? { remaining, dismiss: () => setDismissed(expires) } : null;
  const onContinue = () => {
    setAsked(true);
    void reread.current();
  };
  return (
    <SignInContext.Provider value={{ pending: signingIn, opened: () => setSigningIn(true) }}>
      <WarningContext.Provider value={warning}>
        {children}
        {otherActor ? (
          <OtherActor was={firstActor.current.name} now={session.actor.displayName || shortSubject(session.actor.subject)} />
        ) : ended ? (
          <Ended asked={asked} onContinue={onContinue} />
        ) : null}
      </WarningContext.Provider>
    </SignInContext.Provider>
  );
}

function OtherActor({ was, now }: { was: string; now: string }) {
  return (
    <Modal isOpen variant="small" aria-labelledby="lw-session-other" aria-describedby="lw-session-other-body">
      <ModalHeader title="You signed in as someone else" labelId="lw-session-other" titleIconVariant="warning" />
      <ModalBody id="lw-session-other-body">
        This page was opened as {was}; the session is now {now}. What this page holds was read for the first. Reload it to carry on as
        the second.
      </ModalBody>
      <ModalFooter>
        <Button variant="primary" onClick={() => window.location.reload()}>
          Reload this page
        </Button>
      </ModalFooter>
    </Modal>
  );
}

function Ended({ asked, onContinue }: { asked: boolean; onContinue: () => void }) {
  return (
    <Modal isOpen variant="small" aria-labelledby="lw-session-ended" aria-describedby="lw-session-ended-body">
      <ModalHeader title="Your session ended: sign in again in a new tab, then continue here" labelId="lw-session-ended" titleIconVariant="warning" />
      <ModalBody id="lw-session-ended-body">
        <Content component="p">
          This page stays as it is, with what you typed: nothing was lost and nothing was sent. Sign in in the new tab, come back to this one, and
          continue.
        </Content>
        {asked && (
          <Content component="p" role="status" className="lw-gap-top">
            Still signed out here. Finish signing in in the other tab, then continue.
          </Content>
        )}
      </ModalBody>
      <ModalFooter>
        <SignInNewTab variant="primary" />
        <Button variant="secondary" onClick={onContinue}>
          Continue here
        </Button>
      </ModalFooter>
    </Modal>
  );
}
