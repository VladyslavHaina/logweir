// The signed-in session, read once at boot by the session gate (app/SessionGate.tsx)
// and handed to everything below it. The workspaces, the roles and the capability
// flags all come from GET /api/v1/session; nothing here derives an authorisation.
import { createContext, useContext, type ReactNode } from "react";
import type { Capabilities, WorkspaceGrant, Session } from "./types";

const SessionContext = createContext<Session | null>(null);

export function SessionProvider({ session, children }: { session: Session; children: ReactNode }) {
  return <SessionContext.Provider value={session}>{children}</SessionContext.Provider>;
}

export function useSessionDoc(): Session {
  const session = useContext(SessionContext);
  if (!session) throw new Error("useSessionDoc() outside the session gate");
  return session;
}

/** The grant for `ws`, or undefined when the session holds none there. */
export function grantOf(session: Session, ws: string): WorkspaceGrant | undefined {
  return session.workspaces.find((g) => g.name === ws);
}

export function useGrant(ws: string): WorkspaceGrant | undefined {
  return grantOf(useSessionDoc(), ws);
}

export type Flag = keyof Capabilities;

/** Whether the session may use `flag` in `ws`. A flag is `implemented && allowed`, as the API computed it. */
export function can(session: Session, ws: string, flag: Flag): boolean {
  return grantOf(session, ws)?.capabilities[flag] === true;
}

export function useCan(ws: string, flag: Flag): boolean {
  return can(useSessionDoc(), ws, flag);
}

/** The first workspace the session is granted, where a visit with no workspace lands. */
export function landingWorkspace(session: Session): string | undefined {
  return session.workspaces[0]?.name;
}
