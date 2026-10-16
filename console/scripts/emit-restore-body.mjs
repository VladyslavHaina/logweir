// The laptop walkthrough's Restore (scripts/helm-demo.sh step 6), its plan rendered by the
// console's own renderer.
//
//     node --experimental-strip-types console/scripts/emit-restore-body.mjs --out .demo/laptop/
//
// writes two files and prints three lines:
//
//     <out>/plan.yaml           renderPlanBytes(state.fields), byte for byte
//     <out>/restore-body.json   the Restore object `kubectl create -f` takes
//     plan-hash=sha256:<64 hex>
//     restore-name=restore-<8 hex>
//     approval-name=approval-<8 hex>
//
// THIS FILE CONTAINS NO YAML AND CANNOT. The plan document is `renderPlanBytes` from
// src/pages/restores/plan.ts, the wizard's own renderer, and the two names and the hash
// are its `mintNames` and `planHash` (crates/logweir/tests/laptop_demo_lint.rs holds this
// file to that). The demo creates the Restore with kubectl, not through the product API,
// so the object below is the custom resource; the console sends the same plan bytes to
// `POST .../restores`.
//
// `--fields <path>` (default `<out>/plan-fields.json`) is `{workspace, archiveUrl,
// archiveSecretName, targetClusterName, deadlineSeconds, fields}`, where `fields` is what
// console/fixtures/plan-fields.json holds and `workspace` is the Restore's
// `logweir.dev/workspace` label (the API writes it from the path). It reaches no network.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { mintNames, planHash, renderPlanBytes } from "../src/pages/restores/plan.ts";

function usage(message) {
  process.stderr.write(`emit-restore-body: ${message}\n\nusage: node --experimental-strip-types console/scripts/emit-restore-body.mjs --out <dir> [--fields <path>]\n`);
  process.exit(2);
}

const args = { out: "", fields: "" };
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i += 1) {
  if (argv[i] === "--out") args.out = argv[++i] ?? "";
  else if (argv[i] === "--fields") args.fields = argv[++i] ?? "";
  else usage(`unrecognised argument ${JSON.stringify(argv[i])}`);
}
if (!args.out) usage("--out <dir> is required");
const fieldsPath = args.fields || join(args.out, "plan-fields.json");

let state;
try {
  state = JSON.parse(readFileSync(fieldsPath, "utf8"));
} catch (error) {
  usage(`could not read the wizard state at ${fieldsPath}: ${error.message}`);
}
if (!state.workspace) usage(`${fieldsPath} names no workspace`);

// The bytes are rendered ONCE and every later value is a function of exactly those bytes:
// the sha256 the approver signs and the one the controller recomputes are the same number
// only if there is one document.
const bytes = renderPlanBytes(state.fields);
const hash = await planHash(bytes);
const names = await mintNames(bytes);
const target = state.fields.target ?? {};
const restore = {
  apiVersion: "logweir.dev/v1alpha1",
  kind: "Restore",
  metadata: { name: names.restoreName, labels: { "logweir.dev/workspace": state.workspace } },
  spec: {
    planBytes: bytes,
    approvalRef: { name: names.approvalName },
    sourceArchive: { url: state.archiveUrl, ...(state.archiveSecretName ? { secretRef: { name: state.archiveSecretName } } : {}) },
    backupSetRef: state.fields.backupSetRef,
    pointInTime: state.fields.pointInTime,
    target: { clusterRef: { name: state.targetClusterName }, mode: target.mode, topicNaming: { prefix: target.topicPrefix } },
    deadlineSeconds: typeof state.deadlineSeconds === "number" ? state.deadlineSeconds : 3600,
  },
};

mkdirSync(args.out, { recursive: true });
const planPath = join(args.out, "plan.yaml");
const bodyPath = join(args.out, "restore-body.json");
// The plan file is the bytes, with nothing added: `logweir drill approve --spec` hashes it.
writeFileSync(planPath, bytes);
writeFileSync(bodyPath, `${JSON.stringify(restore, null, 2)}\n`);
process.stdout.write(`plan-hash=${hash}\nrestore-name=${names.restoreName}\napproval-name=${names.approvalName}\n`);
process.stderr.write(`emit-restore-body: wrote ${planPath} and ${bodyPath}\n`);
