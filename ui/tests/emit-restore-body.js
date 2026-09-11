// emit-restore-body.js -- the laptop walkthrough's create body, built by the
// page's own code. Task 28.
//
//     node ui/tests/emit-restore-body.js --out .demo/laptop/
//
// writes two files and prints three lines:
//
//     .demo/laptop/plan.yaml           renderPlanBytes(state.fields), byte for byte
//     .demo/laptop/restore-body.json   restoreBody(state, prepared), the POST body
//     plan-hash=sha256:<64 hex>
//     restore-name=restore-<8 hex>
//     approval-name=approval-<8 hex>
//
// THIS FILE CONTAINS NO YAML AND CANNOT. The plan document is
// `renderPlanBytes` from `../plan.js` -- the wizard's own renderer, interface
// register I19 -- and the two names are `mintNames`, the hash `planHash`. A
// hand-written YAML string here would be a document nobody's page emits: the
// `Restore` would still be created, `logweir drill approve` would still hash
// it, Task 16's five approval checks would still pass, and the RUNNER would
// fail at step 12, three slots after the guard that exists to catch it
// (`render_plan_bytes_emits_a_document_the_runner_parses`, Task 27). So
// `crates/logweir/tests/laptop_demo_lint.rs` asserts, over this file's source,
// that it imports those three from `../plan.js` and that it carries no YAML
// literal.
//
// AND THE BODY IS THE WIZARD'S, NOT A COPY OF IT. `restoreBody` is imported
// from `../pages/restore-wizard.js` -- the same function the "Create the
// Restore" button calls. A second body builder here would be a second thing to
// keep in step with `ArchiveRef`, and the one field it would be easiest to
// forget is the one that took a fix round to find: `spec.sourceArchive.
// secretRef.name`, without which the controller injects no object-store
// credential into the runner Job and the run dies at the archive (plan erratum
// E24g). Importing it makes the scripted half of X-UIWRITE byte-comparable
// with the in-browser half.
//
// THE ARGUMENT IS A WIZARD STATE, NOT A KUBERNETES OBJECT. `--fields <path>`
// (default `<out>/plan-fields.json`) is the object the six steps accumulate:
// `{ns, archiveUrl, archiveSecretName, targetClusterName, deadlineSeconds,
// fields: {...}}`, where `fields` is exactly what
// `ui/tests/fixtures/plan-fields.json` holds. The demo writes that file with
// the values of the archive it has just made -- the endpoint, the region, the
// path style and the evidence bucket are wizard step-1 inputs that NO custom
// resource records (plan erratum E24d), so a live run is the only place they
// are confirmed against the runner.
//
// It is a TOOL, not a test: it registers no test, and
// `scripts/check-ui-behaviour.sh` runs `ui/tests/*.spec.js`, which this file
// deliberately does not match (plan erratum E24g's glob narrowing). It reaches
// no network.

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { mintNames, planHash, renderPlanBytes } from "../plan.js";
import { restoreBody } from "../pages/restore-wizard.js";

function usage(message) {
  process.stderr.write(
    "emit-restore-body: " +
      message +
      "\n\nusage: node ui/tests/emit-restore-body.js --out <dir> [--fields <path>]\n" +
      "\n  --out     directory to write plan.yaml and restore-body.json into\n" +
      "  --fields  the wizard state JSON (default <out>/plan-fields.json)\n",
  );
  process.exit(2);
}

function parseArgs(argv) {
  let out = null;
  let fields = null;
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--out") {
      i += 1;
      out = argv[i];
    } else if (arg === "--fields") {
      i += 1;
      fields = argv[i];
    } else {
      usage("unrecognised argument " + JSON.stringify(arg));
    }
  }
  if (typeof out !== "string" || out.length === 0) {
    usage("--out <dir> is required");
  }
  return { out: out, fields: fields };
}

const args = parseArgs(process.argv.slice(2));
const fieldsPath = args.fields || join(args.out, "plan-fields.json");

let state;
try {
  state = JSON.parse(readFileSync(fieldsPath, "utf8"));
} catch (error) {
  usage("could not read the wizard state at " + fieldsPath + ": " + error.message);
}

// STEP 6 OF THE WIZARD, IN THREE CALLS. The bytes are rendered ONCE and every
// later value is a function of exactly those bytes -- which is the property
// the whole approval flow rests on: the sha256 the approver signs and the
// sha256 the controller recomputes are the same number only if there is one
// document.
const bytes = renderPlanBytes(state.fields);
const hash = await planHash(bytes);
const names = await mintNames(bytes);
const prepared = {
  bytes: bytes,
  hash: hash,
  restoreName: names.restoreName,
  approvalName: names.approvalName,
};

const body = restoreBody(state, prepared);

mkdirSync(args.out, { recursive: true });
const planPath = join(args.out, "plan.yaml");
const bodyPath = join(args.out, "restore-body.json");

// THE PLAN FILE IS THE BYTES, with nothing added. `logweir drill approve
// --spec <this file>` hashes the file's contents, and a trailing newline this
// emitter invented would be a byte the page never showed.
writeFileSync(planPath, bytes);
writeFileSync(bodyPath, JSON.stringify(body, null, 2) + "\n");

process.stdout.write("plan-hash=" + hash + "\n");
process.stdout.write("restore-name=" + prepared.restoreName + "\n");
process.stdout.write("approval-name=" + prepared.approvalName + "\n");

// Keeps the two helpers honest about where they wrote: a run whose `--out` was
// a typo would otherwise print three lines and leave the demo looking for
// files that are somewhere else.
process.stderr.write("emit-restore-body: wrote " + planPath + " and " + bodyPath + "\n");
