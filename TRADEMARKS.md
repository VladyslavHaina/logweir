# Trademarks

Apache Kafka(R) and Kafka(R) are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.

## LOGWEIR is a working name

**LOGWEIR is a working name, not a cleared one.** It was chosen on the evidence
below and it is used here, in the repository, in the crate names and in the
registry namespace — and none of that is a clearance opinion.

**What was searched, and what came back.** TMview, 2026-09-03: the compounds
LOGWEIR and WEIRKEEPER return **zero** records. The stem does not. **"weir"
alone returns 2,824 records**, dominated by WEIR / Weir Engineering Services
Limited — registered in **class 9 in MY**; **classes 1, 6, 7, 9, 11, 17, 37,
40, 42 in KR**; **class 7 in IN and AR**; and others.

**What was not searched.** The UK, EU and US registrations in that result were
**not individually inspected** — the 2,824-record result was only sampled, and
TMview is an aggregator rather than a registry. "Zero records for the compound"
is therefore an encouraging result and not a clearance.

## The clearance act, named

The act that would settle this is **a formal UKIPO + EUIPO + USPTO search for
LOGWEIR and WEIRKEEPER in classes 9 and 42, on the official registries**,
followed by counsel's assessment of likelihood-of-confusion for a "log"+"weir"
compound in software classes against the Weir Group's marks.

**That is owner action, not a task's.** No engineering task in this repository
can produce it, and none claims to. It is recorded here so that the gap is
visible rather than discovered by a letter.

## Announcing is gated on that opinion; tagging is not

**A git tag is a git object. An announcement is a use in commerce.** Creating
`v0.1.0`, pushing this repository and publishing images under the namespace
below are development acts. Announcing the project publicly — a launch post, a
conference talk, a submission to a foundation, an application to register the
mark — is a use in commerce and **is gated on the clearance opinion above**.

## The registry namespace is fixed as a literal, so clearing changes one string

Images publish to `ghcr.io/logweir/logweir` (the runner) and
`ghcr.io/logweir/weirkeeper` (the controller). **Both are literals, not
placeholders.** A shipped `logweir.yaml` carrying `ghcr.io/<org>/logweir@sha256:…`
is not applyable, and the one clause that says a stranger can install Logweir
would fail on exactly the condition a placeholder was meant to protect against.

Fixing the literal now is what makes the trademark question cheap to answer
later: **clearing it, or failing to clear it, changes one string** — the
organisation segment — and changes nothing about the install path, the CRD API
group or the document formats.

## Names Logweir does not use

Per Global Constraint 14, Logweir never publishes under, and never names as its
own:

- The domains `kafkabackup.com` and `oso.sh`.
- The API groups `kafka.oso.sh` and `kafkabackup.com`.
- The `osodevops/` Docker Hub namespace.
- OSO's crates.io package names.

Logweir owns **`logweir.dev/v1alpha1`** — now, not deferred.

Naming upstream's published image in order to *pull* it is a different act from
publishing under it, and is what the pinned digest and the MIT redistribution in
[NOTICE](NOTICE) require. `scripts/check-no-oso.sh` scans for Logweir
*publishing* under those namespaces, not for the literal string.

## Using the Logweir name

Apache-2.0 §6 grants no trademark rights. Until the clearance above has been
done there is no formal trademark policy to point at, so the interim rule is
the ordinary one: describe what you built with Logweir ("built on Logweir",
"Logweir-compatible scorecards") and do not name a fork, a product, a service
or a distribution "Logweir" without asking.

---

Documentation is licensed [CC-BY-4.0](docs/LICENSE-docs).
