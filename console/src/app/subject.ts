// An OIDC subject is an opaque identifier, often long and unbroken (Dex's is a
// base64 protobuf, "CiQxZjBj…BWxvY2Fs"). Where it is shown it is shortened, with
// the whole value kept in a title, so it never spills out of a menu or a page.
export const SUBJECT_MAX = 24;

/**
 * DUI-54.7: Dex's subject is base64 of (field 1: the user id, field 2: the connector
 * id), so its first and last characters are the same for every local user. Decoded,
 * the END of the user id is the part that differs. `null` for any other subject.
 */
export function dexSubject(subject: string): { userId: string; connector: string } | null {
  let bin: string;
  try {
    bin = atob(subject.replace(/-/g, "+").replace(/_/g, "/"));
  } catch {
    return null;
  }
  const n = bin.charCodeAt(0) === 0x0a ? bin.charCodeAt(1) : -1;
  if (n < 1 || bin.charCodeAt(2 + n) !== 0x12) return null;
  const m = bin.charCodeAt(3 + n);
  if (m < 1 || bin.length !== 4 + n + m) return null;
  const userId = bin.slice(2, 2 + n);
  const connector = bin.slice(4 + n);
  return /^[\x21-\x7e]+$/.test(userId + connector) ? { userId, connector } : null;
}

export function shortSubject(subject: string): string {
  if (subject.length <= SUBJECT_MAX) return subject;
  const dex = dexSubject(subject);
  if (dex) {
    const id = dex.userId.length > 13 ? `…${dex.userId.slice(-12)}` : dex.userId;
    return `${id}@${dex.connector}`.slice(0, SUBJECT_MAX);
  }
  return `${subject.slice(0, 10)}…${subject.slice(-8)}`;
}
