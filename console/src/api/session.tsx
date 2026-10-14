// The signed-in session, read once at boot by the session gate (app/SessionGate.tsx)
// and handed to everything below it. The namespaces, the roles and the capability
// flags all come from GET /api/v1/session; nothing here derives an authorisation.
import { createContext, useContext, type ReactNode } from "react";
import type { Capabilities, NamespaceGrant, Session } from "./types";

const SessionContext = createContext<Session | null>(null);

export function SessionProvider({ session, children }: { session: Session; children: ReactNode }) {
  return <SessionContext.Provider value={session}>{children}</SessionContext.Provider>;
}

export function useSessionDoc(): Session {
  const session = useContext(SessionContext);
  if (!session) throw new Error("useSessionDoc() outside the session gate");
  return session;
}

/** The grant for `ns`, or undefined when the session holds none there. */
export function grantOf(session: Session, ns: string): NamespaceGrant | undefined {
  return session.namespaces.find((g) => g.name === ns);
}

export function useGrant(ns: string): NamespaceGrant | undefined {
  return grantOf(useSessionDoc(), ns);
}

export type Flag = keyof Capabilities;

/** Whether the session may use `flag` in `ns`. A flag is `implemented && allowed`, as the API computed it. */
export function can(session: Session, ns: string, flag: Flag): boolean {
  return grantOf(session, ns)?.capabilities[flag] === true;
}

export function useCan(ns: string, flag: Flag): boolean {
  return can(useSessionDoc(), ns, flag);
}

/** The first namespace the session is granted, where a visit with no namespace lands. */
export function landingNamespace(session: Session): string | undefined {
  return session.namespaces[0]?.name;
}
