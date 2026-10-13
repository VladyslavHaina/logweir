// Destinations: the location rules, the grants (create and rotation), what
// "keep" sends, the request bodies and where the API's field errors land.
import { describe, expect, it } from "vitest";
import { ApiError } from "./client";
import { KEY_IN_PLAIN_FIELD, placeProblems } from "./connections";
import {
  DESTINATION_DEFAULTS,
  GRANT_ROLES,
  accessBody,
  destinationBody,
  destinationField,
  destinationProblems,
  emptyGrant,
  forgetAccessCredentials,
  grantProblems,
  keptGrant,
  namedSecrets,
  rotateDefaults,
  rotationBody,
  rotationProblems,
  sourcesFor,
  type Destination,
  type DestinationValues,
  type Grants,
} from "./destinations";
import type { components } from "./schema";

type S = components["schemas"];
const fixtures = import.meta.glob("../../../ui/tests/fixtures/console/*.json", { eager: true, import: "default" });
function fixture<T>(name: string): T {
  const found = fixtures[`../../../ui/tests/fixtures/console/${name}`];
  if (found === undefined) throw new Error(`no fixture ${name}`);
  return structuredClone(found) as T;
}

const KEY_ID = "AKIA-typed-once";
const KEY_SECRET = "secret-typed-once";

function values(over: Partial<DestinationValues> = {}): DestinationValues {
  const v = structuredClone(DESTINATION_DEFAULTS);
  v.name = "primary";
  v.bucket = "kafka-backups";
  v.grants.archiveWrite = { ...emptyGrant("new"), accessKeyId: KEY_ID, secretAccessKey: KEY_SECRET };
  return { ...v, ...over };
}

function grants(over: Partial<Grants>): Grants {
  return { ...rotateDefaults().grants, ...over };
}

const primary = () => fixture<S["DestinationResponse"]>("destination.json").item as Destination;

describe("the location rules", () => {
  it("accepts a complete TLS destination", () => {
    expect(destinationProblems(values({ endpoint: "https://minio.storage.svc:9000" }))).toEqual({});
  });

  it("makes the endpoint's scheme agree with the transport, and changes neither", () => {
    expect(destinationProblems(values({ endpoint: "https://minio:9000", security: "insecureHttp" })).endpoint).toMatch(/https and the transport says plaintext/);
    expect(destinationProblems(values({ endpoint: "http://minio:9000", security: "tls" })).endpoint).toMatch(/plaintext http and the transport says TLS/);
    expect(destinationProblems(values({ endpoint: "", security: "insecureHttp" })).endpoint).toMatch(/no way to reach AWS S3 in the clear/);
    // Controls: each consistent pair is accepted.
    expect(destinationProblems(values({ endpoint: "http://minio:9000", security: "insecureHttp" })).endpoint).toBeUndefined();
    expect(destinationProblems(values({ endpoint: "", security: "tls" })).endpoint).toBeUndefined();
  });

  it("takes an endpoint as an origin only", () => {
    for (const bad of ["minio:9000", "https://minio:9000/path", "https://user@minio:9000", "https://minio:9000?x=1"]) {
      expect(destinationProblems(values({ endpoint: bad })).endpoint, bad).toMatch(/origin/);
    }
  });

  it("refuses virtual-hosted addressing with a custom endpoint, and allows it on AWS S3", () => {
    expect(destinationProblems(values({ endpoint: "https://minio:9000", addressing: "virtualHosted" })).addressing).toMatch(/refused/);
    expect(destinationProblems(values({ endpoint: "", addressing: "virtualHosted" })).addressing).toBeUndefined();
  });

  it("reserves logweir/ for evidence and keeps prefixes relative", () => {
    expect(destinationProblems(values({ prefix: "logweir" })).prefix).toMatch(/reserved for evidence/);
    expect(destinationProblems(values({ prefix: "logweir/archive" })).prefix).toMatch(/reserved for evidence/);
    expect(destinationProblems(values({ prefix: "../other" })).prefix).toMatch(/relative/);
    expect(destinationProblems(values({ prefix: "logweir-archive/team-a" })).prefix).toBeUndefined();
  });

  it("takes S3 bucket names and a CA bundle only over TLS", () => {
    expect(destinationProblems(values({ bucket: "Kafka_Backups" })).bucket).toBeDefined();
    expect(destinationProblems(values({ bucket: "kb" })).bucket).toBeDefined();
    expect(destinationProblems(values({ caName: "minio-ca", endpoint: "http://minio:9000", security: "insecureHttp" })).caName).toMatch(/TLS transport/);
    expect(destinationProblems(values({ caName: "minio-ca", endpoint: "https://minio:9000" })).caName).toBeUndefined();
  });

  it("refuses private-key text in the fields that are stored and shown back", () => {
    const key = "-----BEGIN PRIVATE KEY-----\nMIG\n";
    expect(destinationProblems(values({ description: key })).description).toBe(KEY_IN_PLAIN_FIELD);
    expect(destinationProblems(values({ description: "the team-a production archive" })).description).toBeUndefined();
  });
});

describe("the grants", () => {
  it("offers a new destination no existing Secret, and archive write no absence", () => {
    expect(sourcesFor("archiveWrite", "create")).toEqual(["new", "workloadIdentity"]);
    expect(sourcesFor("evidenceRead", "create")).toEqual(["absent", "new", "workloadIdentity", "controllerIdentity", "archiveReadGrant"]);
    expect(sourcesFor("archiveRead", "rotate")[0]).toBe("keep");
    expect(sourcesFor("archiveRead", "rotate")).toContain("existing");
  });

  it("requires archive write, both halves of a new credential, and keeps evidence-read answers to evidence read", () => {
    const p = grantProblems({
      archiveWrite: emptyGrant("absent"),
      archiveRead: { ...emptyGrant("new"), accessKeyId: KEY_ID },
      evidenceWrite: emptyGrant("controllerIdentity"),
      evidenceRead: emptyGrant("controllerIdentity"),
    });
    expect(p["grants.archiveWrite.source"]).toMatch(/required/);
    expect(p["grants.archiveRead.secretAccessKey"]).toBeDefined();
    expect(p["grants.archiveRead.accessKeyId"]).toBeUndefined();
    expect(p["grants.evidenceWrite.source"]).toMatch(/evidence-read answer only/);
    expect(p["grants.evidenceRead.source"]).toBeUndefined();
    expect(JSON.stringify(p)).not.toContain(KEY_ID);
  });

  it("lets a rotation name only a Secret the destination already names", () => {
    const d = primary();
    const names = namedSecrets(d.access);
    expect(names).toEqual(["logweir-s3", "lwd-primary-archive-read"]);
    expect(grantProblems(grants({ archiveRead: { ...emptyGrant("existing"), secret: "someone-elses" } }), d.access, names)["grants.archiveRead.secret"]).toBeDefined();
    expect(grantProblems(grants({ archiveRead: { ...emptyGrant("existing"), secret: "logweir-s3" } }), d.access, names)).toEqual({});
  });
});

describe("keep", () => {
  it("sends each grant as the destination holds it", () => {
    const a = primary().access;
    expect(keptGrant(a.archiveWrite)).toEqual({ mode: "secretKeys", secret: { existing: { name: "logweir-s3" } } });
    expect(keptGrant(a.evidenceWrite)).toBeNull();
    expect(keptGrant(a.evidenceRead)).toEqual({ mode: "archiveReadGrant" });
    expect(keptGrant({ mode: "secretKeys", secretName: "s", keys: ["access-key-id", "secret-access-key", "session-token"] })).toEqual({
      mode: "secretKeys",
      secret: { existing: { name: "s", sessionTokenKey: "session-token" } },
    });
    expect(keptGrant({ mode: "workloadIdentity", serviceAccountName: "runner", keys: [] })).toEqual({ mode: "workloadIdentity", workloadIdentity: { serviceAccountName: "runner" } });
  });

  it("refuses keep when it cannot say which key holds the key id", () => {
    const odd: S["AccessGrantView"] = { mode: "secretKeys", secretName: "odd", keys: ["id", "key"] };
    expect(keptGrant(odd)).toBeUndefined();
    const d = primary();
    d.access.archiveRead = odd;
    expect(grantProblems(grants({}), d.access)["grants.archiveRead.source"]).toMatch(/Keep cannot be sent/);
    expect(grantProblems(grants({}), primary().access)).toEqual({});
  });
});

describe("the rotation request", () => {
  it("sends the complete block with every grant kept, and the generation this page read", () => {
    const d = primary();
    const body = rotationBody(rotateDefaults(), d);
    expect(body.expectedGeneration).toBe(2);
    expect(Object.keys(body.access).sort()).toEqual(["archiveRead", "archiveWrite", "evidenceRead"]);
    expect(body.access.archiveRead).toEqual({ mode: "secretKeys", secret: { existing: { name: "lwd-primary-archive-read" } } });
    expect(body.transport).toBeUndefined();
  });

  it("removes a grant set to absent, and only that one", () => {
    const body = rotationBody({ ...rotateDefaults(), grants: grants({ evidenceRead: emptyGrant("absent") }) }, primary());
    expect(body.access.evidenceRead).toBeUndefined();
    expect(body.access.archiveRead).toBeDefined();
  });

  it("clears the CA with an empty transport, sets it with a bundle, and leaves it alone by default", () => {
    expect(rotationBody({ ...rotateDefaults(), caChange: "clear" }, primary()).transport).toEqual({});
    expect(rotationBody({ ...rotateDefaults(), caChange: "set", caName: "new-ca", caKey: "" }, primary()).transport).toEqual({ caBundle: { configMapName: "new-ca" } });
    const plaintext = { ...primary(), transport: { security: "insecureHttp" as const } };
    expect(rotationProblems({ ...rotateDefaults(), caChange: "set", caName: "new-ca" }, plaintext).caName).toMatch(/plaintext/);
  });
});

describe("the create request", () => {
  it("reads addressing and transport apart: neither implies the other", () => {
    const a = destinationBody(values({ addressing: "virtualHosted", endpoint: "" }));
    expect(a.storage.addressing).toBe("virtualHosted");
    expect(a.transport.security).toBe("tls");
    const b = destinationBody(values({ security: "insecureHttp", endpoint: "http://minio:9000" }));
    expect(b.storage.addressing).toBe("pathStyle");
    expect(b.transport).toEqual({ security: "insecureHttp" });
  });

  it("types the credential once into the request and drops it after sending", () => {
    const body = destinationBody(values({ isDefault: true, description: " archive " }));
    expect(body.access.archiveWrite.secret?.new).toEqual({ accessKeyId: KEY_ID, secretAccessKey: KEY_SECRET });
    expect(body.access.archiveRead).toBeUndefined();
    expect(body).toMatchObject({ default: true, description: "archive", readiness: { writeProbe: "createOnlyMarker" } });
    forgetAccessCredentials(body.access);
    expect(JSON.stringify(body)).not.toContain(KEY_SECRET);
    expect(JSON.stringify(body)).not.toContain(KEY_ID);
    expect(body.access.archiveWrite.mode).toBe("secretKeys");
  });

  it("sends absent as no member, never as inheritsArchiveWrite", () => {
    const access = accessBody(values().grants);
    for (const role of GRANT_ROLES.slice(1)) expect(access[role]).toBeUndefined();
    expect(JSON.stringify(access)).not.toMatch(/inheritsArchiveWrite|notConfigured/);
  });
});

describe("where the API's field errors land", () => {
  it("maps grant, location and transport paths to their fields", () => {
    expect(destinationField("access.archiveWrite.secret.new.accessKeyId")).toBe("grants.archiveWrite.accessKeyId");
    expect(destinationField("access.evidenceRead.secret.existing")).toBe("grants.evidenceRead.secret");
    expect(destinationField("access.archiveRead.mode")).toBe("grants.archiveRead.source");
    expect(destinationField("storage.endpoint")).toBe("endpoint");
    expect(destinationField("transport.caBundle.key")).toBe("caKey");
    expect(destinationField("readiness.writeProbe")).toBe("writeProbe");
    expect(destinationField("somethingElse")).toBeUndefined();
    const placed = placeProblems(
      new ApiError(422, { code: "destination_invalid", errors: [{ field: "storage.bucket", code: "bucket_invalid", message: "is not an S3 bucket name" }, { field: "x", code: "y", message: "z" }] }),
      destinationField,
    );
    expect(placed).toEqual({ fields: { bucket: "is not an S3 bucket name" }, unmatched: ["x: z"] });
  });
});
