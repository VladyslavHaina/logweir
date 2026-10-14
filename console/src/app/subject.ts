// An OIDC subject is an opaque identifier, often long and unbroken (Dex's is a
// base64 protobuf, "CiQxZjBj…BWxvY2Fs"). Where it is shown it is shortened, with
// the whole value kept in a title, so it never spills out of a menu or a page.
export const SUBJECT_MAX = 24;

export function shortSubject(subject: string): string {
  return subject.length <= SUBJECT_MAX ? subject : `${subject.slice(0, 10)}…${subject.slice(-8)}`;
}
