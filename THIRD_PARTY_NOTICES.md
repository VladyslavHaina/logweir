# Third-party notices

**Generated. Regenerated, never edited.** Every entry below is produced by
`scripts/gen-third-party-notices.sh`, which runs
`cargo metadata --format-version 1 --offline` against the committed `Cargo.lock`
and reaches no network. To refresh it:

```bash
bash scripts/gen-third-party-notices.sh --write
```

`crates/logweir/tests/doc_lint.rs::third_party_notices_covers_every_resolved_package`
re-derives the expected set from `Cargo.lock` and fails naming any `name@version`
this file has lost, so a hand edit is caught by a test that reads the lockfile
rather than this document.

**Packages in the resolved graph: 392.**

## What this file is, and what `deny.toml` is

SPDX cleanliness is a **permission** check — may Logweir ship under Apache-2.0 at
all — and `cargo deny check licenses --offline` answers it. This file answers the
other half: MIT, BSD-2-Clause, BSD-3-Clause and Apache-2.0 each require the
**copyright notice to travel with the redistributed binary**, and Logweir
redistributes a statically linked binary in two container images and in release
tarballs. Global Constraint 15; `docs/research/R10-naming-trademark-license.md`
states the obligation for this exact case.

It is **not** the whole of the attribution Logweir owes. The C code statically
linked through `rdkafka-sys`'s `cmake-build` feature — librdkafka, its fourteen
vendored components, and OpenSSL — is invisible to `cargo metadata` and is
attributed in [NOTICE](NOTICE) instead.

## The copyright line has three sources, and each entry names the one it used

1. **licence file** — a `LICENSE*`, `COPYRIGHT*` or `NOTICE*` file beside the crate's own
   `Cargo.toml`, first line matching `^\s*Copyright` **and** carrying a `(c)`,
   `(C)`, `©` or four-digit year. The crate author's own words, always
   preferred. The extra condition is not fussiness: without it the match hits
   Apache-2.0's own body text and its `Copyright [yyyy] [name of copyright
   owner]` placeholder, and every crate shipping `LICENSE-APACHE` is attributed
   to a fragment of the licence it ships.
2. **`authors` field** — the manifest's `authors`, used only when arm 1 finds nothing. An
   author is not a copyright holder; the entry says which arm it used so that
   the difference is visible rather than implied.
3. **neither; the fact is stated** — the published crate carries neither, so the entry says so in
   words, with the SPDX expression that governs it regardless. A generator that
   emitted an empty field here would produce a file that looks complete and is
   not: a reader could not tell "nothing is owed" from "the tool found
   nothing".
4. **this workspace** — Logweir's own crates, taking [NOTICE](NOTICE) line 2 byte for byte.

| arm | entries |
|---|---|
| licence file | 297 |
| `authors` field | 69 |
| neither; the fact is stated | 16 |
| this workspace | 10 |

## The inventory

### adler2@2.0.1

- SPDX: `0BSD OR MIT OR Apache-2.0`
- Copyright: Copyright (C) Jonas Schievink <jonasschievink@gmail.com>
- Copyright source: licence file

### ahash@0.8.12

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018 Tom Kaitchuck
- Copyright source: licence file

### aho-corasick@1.1.5

- SPDX: `Unlicense OR MIT`
- Copyright: Copyright (c) 2015 Andrew Gallant
- Copyright source: licence file

### allocator-api2@0.2.21

- SPDX: `MIT OR Apache-2.0`
- Copyright: Zakarum <zaq.dev@icloud.com>
- Copyright source: `authors` field

### android_system_properties@0.1.6

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2016 Nicolas Silva
- Copyright source: licence file

### anstream@0.6.21

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### anstyle@1.0.14

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### anstyle-parse@0.2.7

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### anstyle-query@1.1.5

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### anstyle-wincon@3.0.11

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### async-broadcast@0.7.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2020 Yoshua Wuyts
- Copyright source: licence file

### async-stream@0.3.6

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Carl Lerche
- Copyright source: licence file

### async-stream-impl@0.3.6

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Carl Lerche
- Copyright source: licence file

### async-trait@0.1.92

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### atomic-waker@1.1.2

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2016 Alex Crichton
- Copyright source: licence file

### autocfg@1.5.1

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2018 Josh Stone
- Copyright source: licence file

### aws-lc-rs@1.18.1

- SPDX: `ISC AND (Apache-2.0 OR ISC)`
- Copyright: AWS-LibCrypto
- Copyright source: `authors` field

### aws-lc-sys@0.45.0

- SPDX: `ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause AND (Apache-2.0 OR ISC OR MIT) AND (Apache-2.0 OR ISC OR MIT-0)`
- Copyright: Copyright (c) 2014-2024 Google Inc.
- Copyright source: licence file

### backon@1.6.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2021 Datafuse Labs
- Copyright source: licence file

### base16ct@0.2.0

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2014 Steve "Sc00bz" Thomas (steve at tobtu dot com)
- Copyright source: licence file

### base64@0.22.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015 Alice Maz
- Copyright source: licence file

### base64ct@1.7.3

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2014 Steve "Sc00bz" Thomas (steve at tobtu dot com)
- Copyright source: licence file

### bindgen@0.72.1

- SPDX: `BSD-3-Clause`
- Copyright: Copyright (c) 2013, Jyun-Yan You
- Copyright source: licence file

### bitflags@2.13.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 The Rust Project Developers
- Copyright source: licence file

### block-buffer@0.10.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018-2019 The RustCrypto Project Developers
- Copyright source: licence file

### block-buffer@0.12.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018-2025 The RustCrypto Project Developers
- Copyright source: licence file

### bumpalo@3.20.3

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2019 Nick Fitzgerald
- Copyright source: licence file

### byteorder@1.5.0

- SPDX: `Unlicense OR MIT`
- Copyright: Copyright (c) 2015 Andrew Gallant
- Copyright source: licence file

### bytes@1.12.1

- SPDX: `MIT`
- Copyright: Copyright (c) 2018 Carl Lerche
- Copyright source: licence file

### cc@1.4.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### cexpr@0.6.0

- SPDX: `Apache-2.0/MIT`
- Copyright: Jethro Beekman <jethro@jbeekman.nl>
- Copyright source: `authors` field

### cfg-if@1.0.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### cfg_aliases@0.2.2

- SPDX: `MIT`
- Copyright: Copyright (c) 2020 Katharos Technology
- Copyright source: licence file

### chacha20@0.10.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2019-2026 The RustCrypto Project Developers
- Copyright source: licence file

### chrono@0.4.45

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014, Kang Seonghoon.
- Copyright source: licence file

### clang-sys@1.9.1

- SPDX: `Apache-2.0`
- Copyright: Kyle Mayes <kyle@mayeses.com>
- Copyright source: `authors` field

### clap@4.5.40

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### clap_builder@4.5.40

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### clap_derive@4.5.40

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### clap_lex@0.7.7

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### cmake@0.1.58

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### colorchoice@1.0.5

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### combine@4.6.8

- SPDX: `MIT`
- Copyright: Copyright (c) 2015 Markus Westerlind
- Copyright source: licence file

### console@0.16.4

- SPDX: `MIT`
- Copyright: Copyright (c) 2017 Armin Ronacher <armin.ronacher@active-4.com>
- Copyright source: licence file

### const-oid@0.9.6

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2020-2022 The RustCrypto Project Developers
- Copyright source: licence file

### core-foundation@0.10.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2012-2013 Mozilla Foundation
- Copyright source: licence file

### core-foundation-sys@0.8.7

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2012-2013 Mozilla Foundation
- Copyright source: licence file

### cpufeatures@0.2.17

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2020-2025 The RustCrypto Project Developers
- Copyright source: licence file

### cpufeatures@0.3.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2020-2026 The RustCrypto Project Developers
- Copyright source: licence file

### crc-fast@1.10.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2025 Don MacAskill
- Copyright source: licence file

### crc32fast@1.5.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018 Sam Rijs, Alex Crichton and contributors
- Copyright source: licence file

### crypto-bigint@0.5.5

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2021 The RustCrypto Project Developers
- Copyright source: licence file

### crypto-common@0.1.7

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2021 RustCrypto Developers
- Copyright source: licence file

### crypto-common@0.2.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2021-2026 RustCrypto Developers
- Copyright source: licence file

### curve25519-dalek@4.1.3

- SPDX: `BSD-3-Clause`
- Copyright: Copyright (c) 2016-2021 isis agora lovecruft. All rights reserved.
- Copyright source: licence file

### curve25519-dalek-derive@0.1.1

- SPDX: `MIT/Apache-2.0`
- Copyright: no copyright statement in the published crate; SPDX MIT/Apache-2.0 applies
- Copyright source: neither; the fact is stated

### darling@0.20.11

- SPDX: `MIT`
- Copyright: Copyright (c) 2017 Ted Driggs
- Copyright source: licence file

### darling_core@0.20.11

- SPDX: `MIT`
- Copyright: Copyright (c) 2017 Ted Driggs
- Copyright source: licence file

### darling_macro@0.20.11

- SPDX: `MIT`
- Copyright: Copyright (c) 2017 Ted Driggs
- Copyright source: licence file

### der@0.7.10

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2020-2023 The RustCrypto Project Developers
- Copyright source: licence file

### digest@0.10.7

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2017 Artyom Pavlov
- Copyright source: licence file

### digest@0.11.3

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2017-2025 RustCrypto Developers
- Copyright source: licence file

### displaydoc@0.2.7

- SPDX: `MIT OR Apache-2.0`
- Copyright: Jane Lusby <jlusby@yaah.dev>
- Copyright source: `authors` field

### duct@0.13.7

- SPDX: `MIT`
- Copyright: oconnor663@gmail.com
- Copyright source: `authors` field

### dunce@1.0.5

- SPDX: `CC0-1.0 OR MIT-0 OR Apache-2.0`
- Copyright: Kornel <kornel@geekhood.net>
- Copyright source: `authors` field

### dyn-clone@1.0.20

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### e2e@0.1.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2026 The Logweir Authors
- Copyright source: this workspace

### ecdsa@0.16.9

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright 2018-2022 RustCrypto Developers
- Copyright source: licence file

### ed25519@2.2.3

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright 2018-2022 RustCrypto Developers
- Copyright source: licence file

### ed25519-dalek@2.2.0

- SPDX: `BSD-3-Clause`
- Copyright: Copyright (c) 2017-2019 isis agora lovecruft. All rights reserved.
- Copyright source: licence file

### educe@0.6.0

- SPDX: `MIT`
- Copyright: Copyright (c) 2023 magiclen.org (Ron Li)
- Copyright source: licence file

### either@1.18.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015
- Copyright source: licence file

### elliptic-curve@0.13.8

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2020-2022 RustCrypto Developers
- Copyright source: licence file

### encode_unicode@1.0.0

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Torbjørn Birch Moltu <t.b.moltu@lyse.net>
- Copyright source: `authors` field

### enum-ordinalize@4.4.2

- SPDX: `MIT`
- Copyright: Copyright (c) 2023 magiclen.org (Ron Li)
- Copyright source: licence file

### enum-ordinalize-derive@4.4.2

- SPDX: `MIT`
- Copyright: Copyright (c) 2023 magiclen.org (Ron Li)
- Copyright source: licence file

### equivalent@1.0.2

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2016--2023
- Copyright source: licence file

### errno@0.3.14

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Chris Wong
- Copyright source: licence file

### event-listener@5.4.2

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Stjepan Glavina <stjepang@gmail.com>, John Nunley <dev@notgull.net>
- Copyright source: `authors` field

### event-listener-strategy@0.5.4

- SPDX: `Apache-2.0 OR MIT`
- Copyright: John Nunley <dev@notgull.net>
- Copyright source: `authors` field

### fastrand@2.5.0

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Stjepan Glavina <stjepang@gmail.com>
- Copyright source: `authors` field

### ff@0.13.1

- SPDX: `MIT/Apache-2.0`
- Copyright: Copyright (c) 2017 Sean Bowe
- Copyright source: licence file

### fiat-crypto@0.2.9

- SPDX: `MIT OR Apache-2.0 OR BSD-1-Clause`
- Copyright: Copyright 2015-2020 the fiat-crypto authors (see the AUTHORS file)
- Copyright source: licence file

### find-msvc-tools@0.1.11

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### flate2@1.1.10

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014-2026 Alex Crichton
- Copyright source: licence file

### fnv@1.0.7

- SPDX: `Apache-2.0 / MIT`
- Copyright: Copyright (c) 2017 Contributors
- Copyright source: licence file

### foldhash@0.1.5

- SPDX: `Zlib`
- Copyright: Copyright (c) 2024 Orson Peters
- Copyright source: licence file

### form_urlencoded@1.2.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2013-2016 The rust-url developers
- Copyright source: licence file

### fs_extra@1.3.0

- SPDX: `MIT`
- Copyright: Copyright (c) 2017 Denis Kurilenko
- Copyright source: licence file

### futures@0.3.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Alex Crichton
- Copyright source: licence file

### futures-channel@0.3.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Alex Crichton
- Copyright source: licence file

### futures-core@0.3.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Alex Crichton
- Copyright source: licence file

### futures-executor@0.3.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Alex Crichton
- Copyright source: licence file

### futures-io@0.3.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Alex Crichton
- Copyright source: licence file

### futures-macro@0.3.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Alex Crichton
- Copyright source: licence file

### futures-sink@0.3.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Alex Crichton
- Copyright source: licence file

### futures-task@0.3.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Alex Crichton
- Copyright source: licence file

### futures-util@0.3.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Alex Crichton
- Copyright source: licence file

### generic-array@0.14.7

- SPDX: `MIT`
- Copyright: Copyright (c) 2015 Bartłomiej Kamiński
- Copyright source: licence file

### getrandom@0.2.17

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018-2024 The rust-random Project Developers
- Copyright source: licence file

### getrandom@0.3.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018-2025 The rust-random Project Developers
- Copyright source: licence file

### getrandom@0.4.3

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018-2026 The rust-random Project Developers
- Copyright source: licence file

### glob@0.3.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 The Rust Project Developers
- Copyright source: licence file

### gloo-timers@0.3.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Rust and WebAssembly Working Group
- Copyright source: `authors` field

### group@0.13.0

- SPDX: `MIT/Apache-2.0`
- Copyright: Sean Bowe <ewillbefull@gmail.com>, Jack Grigg <jack@z.cash>
- Copyright source: `authors` field

### h2@0.4.19

- SPDX: `MIT`
- Copyright: Copyright (c) 2017 h2 authors
- Copyright source: licence file

### hashbrown@0.15.5

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Amanieu d'Antras
- Copyright source: licence file

### hashbrown@0.16.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Amanieu d'Antras
- Copyright source: licence file

### headers@0.4.1

- SPDX: `MIT`
- Copyright: Copyright (c) 2014-2025 Sean McArthur
- Copyright source: licence file

### headers-core@0.3.0

- SPDX: `MIT`
- Copyright: Copyright (c) 2014-2023 Sean McArthur
- Copyright source: licence file

### heck@0.5.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015 The Rust Project Developers
- Copyright source: licence file

### hex@0.4.3

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2013-2014 The Rust Project Developers.
- Copyright source: licence file

### hmac@0.12.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2017 Artyom Pavlov
- Copyright source: licence file

### home@0.5.12

- SPDX: `MIT OR Apache-2.0`
- Copyright: Brian Anderson <andersrb@gmail.com>
- Copyright source: `authors` field

### hostname@0.4.2

- SPDX: `MIT`
- Copyright: Copyright (c) 2016 fengcen
- Copyright source: licence file

### http@1.5.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2017 http-rs authors
- Copyright source: licence file

### http-body@1.1.0

- SPDX: `MIT`
- Copyright: Copyright (c) 2019-2026 Sean McArthur & Hyper Contributors
- Copyright source: licence file

### http-body-util@0.1.5

- SPDX: `MIT`
- Copyright: Copyright (c) 2019-2026 Sean McArthur & Hyper Contributors
- Copyright source: licence file

### httparse@1.10.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015-2025 Sean McArthur
- Copyright source: licence file

### httpdate@1.0.3

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Pyfisch
- Copyright source: licence file

### humantime@2.4.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 The humantime Developers
- Copyright source: licence file

### hybrid-array@0.4.14

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2022-2026 The RustCrypto Project Developers
- Copyright source: licence file

### hyper@1.11.1

- SPDX: `MIT`
- Copyright: Copyright (c) 2014-2026 Sean McArthur
- Copyright source: licence file

### hyper-http-proxy@1.2.0

- SPDX: `MIT`
- Copyright: Copyright (c) 2017 Johann Tuffe
- Copyright source: licence file

### hyper-rustls@0.27.9

- SPDX: `Apache-2.0 OR ISC OR MIT`
- Copyright: Copyright (c) 2016, Joseph Birr-Pixton <jpixton@gmail.com>
- Copyright source: licence file

### hyper-timeout@0.5.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 The weldr Project Developers
- Copyright source: licence file

### hyper-util@0.1.20

- SPDX: `MIT`
- Copyright: Copyright (c) 2023-2025 Sean McArthur
- Copyright source: licence file

### iana-time-zone@0.1.65

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2020 Andrew Straw
- Copyright source: licence file

### iana-time-zone-haiku@0.1.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2020 Andrew Straw
- Copyright source: licence file

### icu_collections@2.3.0

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### icu_locale_core@2.3.0

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### icu_normalizer@2.3.0

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### icu_normalizer_data@2.3.0

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### icu_properties@2.3.0

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### icu_properties_data@2.3.0

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### icu_provider@2.3.1

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### ident_case@1.0.1

- SPDX: `MIT/Apache-2.0`
- Copyright: Ted Driggs <ted.driggs@outlook.com>
- Copyright source: `authors` field

### idna@1.1.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2013-2025 The rust-url developers
- Copyright source: licence file

### idna_adapter@1.2.2

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) The rust-url developers
- Copyright source: licence file

### indexmap@2.13.1

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2016--2017
- Copyright source: licence file

### insta@1.48.0

- SPDX: `Apache-2.0`
- Copyright: Armin Ronacher <armin.ronacher@active-4.com>
- Copyright source: `authors` field

### ipnet@2.12.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2017 Juniper Networks, Inc.
- Copyright source: licence file

### is_terminal_polyfill@1.70.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### itertools@0.13.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015
- Copyright source: licence file

### itertools@0.15.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015
- Copyright source: licence file

### itoa@1.0.18

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### jni@0.22.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: jni team
- Copyright source: `authors` field

### jni-macros@0.22.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: no copyright statement in the published crate; SPDX MIT OR Apache-2.0 applies
- Copyright source: neither; the fact is stated

### jni-sys@0.4.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015 The rust-jni-sys Developers
- Copyright source: licence file

### jni-sys-macros@0.4.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Robert Bragg <robert@sixbynine.org>
- Copyright source: `authors` field

### jobserver@0.1.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### js-sys@0.3.104

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### json-patch@4.2.0

- SPDX: `MIT/Apache-2.0`
- Copyright: Copyright (c) 2017 Ivan Dubrov
- Copyright source: licence file

### jsonpath-rust@0.7.5

- SPDX: `MIT`
- Copyright: Copyright (c) [2021] [Boris Zhguchev]
- Copyright source: licence file

### jsonptr@0.7.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2024 Chance Dinkins
- Copyright source: licence file

### k8s-openapi@0.24.0

- SPDX: `Apache-2.0`
- Copyright: Arnav Singh <me@arnavion.dev>
- Copyright source: `authors` field

### kube@0.99.0

- SPDX: `Apache-2.0`
- Copyright: clux <sszynrae@gmail.com>, Natalie Klestrup Röijezon <nat@nullable.se>, kazk <kazk.dev@gmail.com>
- Copyright source: `authors` field

### kube-client@0.99.0

- SPDX: `Apache-2.0`
- Copyright: clux <sszynrae@gmail.com>, Natalie Klestrup Röijezon <nat@nullable.se>, kazk <kazk.dev@gmail.com>
- Copyright source: `authors` field

### kube-core@0.99.0

- SPDX: `Apache-2.0`
- Copyright: clux <sszynrae@gmail.com>, Natalie Klestrup Röijezon <nat@nullable.se>, kazk <kazk.dev@gmail.com>
- Copyright source: `authors` field

### kube-derive@0.99.0

- SPDX: `Apache-2.0`
- Copyright: clux <sszynrae@gmail.com>, Natalie Klestrup Röijezon <nat@nullable.se>, kazk <kazk.dev@gmail.com>
- Copyright source: `authors` field

### kube-runtime@0.99.0

- SPDX: `Apache-2.0`
- Copyright: clux <sszynrae@gmail.com>, Natalie Klestrup Röijezon <nat@nullable.se>, kazk <kazk.dev@gmail.com>
- Copyright source: `authors` field

### lazy_static@1.5.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2010 The Rust Project Developers
- Copyright source: licence file

### libc@0.2.189

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) The Rust Project Developers
- Copyright source: licence file

### libloading@0.8.9

- SPDX: `ISC`
- Copyright: Copyright © 2015, Simonas Kazlauskas
- Copyright source: licence file

### libz-sys@1.1.29

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### linux-raw-sys@0.12.1

- SPDX: `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT`
- Copyright: Dan Gohman <dev@sunfishcode.online>
- Copyright source: `authors` field

### litemap@0.8.3

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### lock_api@0.4.14

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 The Rust Project Developers
- Copyright source: licence file

### log@0.4.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 The Rust Project Developers
- Copyright source: licence file

### logweir@0.1.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2026 The Logweir Authors
- Copyright source: this workspace

### logweir-core@0.1.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2026 The Logweir Authors
- Copyright source: this workspace

### logweir-engine-oso@0.1.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2026 The Logweir Authors
- Copyright source: this workspace

### logweir-evidence@0.1.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2026 The Logweir Authors
- Copyright source: this workspace

### logweir-kafka@0.1.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2026 The Logweir Authors
- Copyright source: this workspace

### logweir-store@0.1.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2026 The Logweir Authors
- Copyright source: this workspace

### logweir-verify@0.1.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2026 The Logweir Authors
- Copyright source: this workspace

### lru-slab@0.1.2

- SPDX: `MIT OR Apache-2.0 OR Zlib`
- Copyright: Copyright (c) 2024 The lru-slab Developers
- Copyright source: licence file

### lz4_flex@0.13.1

- SPDX: `MIT`
- Copyright: Copyright (c) 2020 Pascal Seitz
- Copyright source: licence file

### matchers@0.2.0

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Eliza Weisman
- Copyright source: licence file

### md-5@0.11.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016-2026 The RustCrypto Project Developers
- Copyright source: licence file

### memchr@2.8.3

- SPDX: `Unlicense OR MIT`
- Copyright: Copyright (c) 2015 Andrew Gallant
- Copyright source: licence file

### mime@0.3.17

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Sean McArthur
- Copyright source: licence file

### minimal-lexical@0.2.1

- SPDX: `MIT/Apache-2.0`
- Copyright: Copyright (c) 2009 The Go Authors. All rights reserved.
- Copyright source: licence file

### miniz_oxide@0.9.1

- SPDX: `MIT OR Zlib OR Apache-2.0`
- Copyright: Copyright 2013-2014 RAD Game Tools and Valve Software
- Copyright source: licence file

### mio@1.2.3

- SPDX: `MIT`
- Copyright: Copyright (c) 2014 Carl Lerche and other MIO contributors
- Copyright source: licence file

### nix@0.31.3

- SPDX: `MIT`
- Copyright: Copyright (c) 2015 Carl Lerche + nix-rust Authors
- Copyright source: licence file

### nom@7.1.3

- SPDX: `MIT`
- Copyright: Copyright (c) 2014-2019 Geoffroy Couprie
- Copyright source: licence file

### nu-ansi-term@0.50.3

- SPDX: `MIT`
- Copyright: Copyright (c) 2014 Benjamin Sago
- Copyright source: licence file

### num-traits@0.2.19

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 The Rust Project Developers
- Copyright source: licence file

### num_enum@0.7.6

- SPDX: `BSD-3-Clause OR MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018, Daniel Wagner-Hall
- Copyright source: licence file

### num_enum_derive@0.7.6

- SPDX: `BSD-3-Clause OR MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018, Daniel Wagner-Hall
- Copyright source: licence file

### object_store@0.14.1

- SPDX: `MIT/Apache-2.0`
- Copyright: Copyright 2020-2026 The Apache Software Foundation
- Copyright source: licence file

### once_cell@1.21.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Aleksey Kladov <aleksey.kladov@gmail.com>
- Copyright source: `authors` field

### once_cell_polyfill@1.70.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### openssl-probe@0.2.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### openssl-sys@0.9.117

- SPDX: `MIT`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### ordered-float@2.10.1

- SPDX: `MIT`
- Copyright: Copyright (c) 2015 Jonathan Reem
- Copyright source: licence file

### os_pipe@1.2.3

- SPDX: `MIT`
- Copyright: Jack O'Connor
- Copyright source: `authors` field

### p256@0.13.2

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2020-2023 RustCrypto Developers
- Copyright source: licence file

### parking@2.2.1

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright 2014-2020 The Rust Project Developers
- Copyright source: licence file

### parking_lot@0.12.5

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 The Rust Project Developers
- Copyright source: licence file

### parking_lot_core@0.9.12

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 The Rust Project Developers
- Copyright source: licence file

### pem@3.0.6

- SPDX: `MIT`
- Copyright: Copyright (c) 2016 Jonathan Creekmore
- Copyright source: licence file

### pem-rfc7468@0.7.0

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2021 The RustCrypto Project Developers
- Copyright source: licence file

### percent-encoding@2.3.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2013-2025 The rust-url developers
- Copyright source: licence file

### pest@2.9.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Dragoș Tiselice <dragostiselice@gmail.com>
- Copyright source: `authors` field

### pest_derive@2.9.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Dragoș Tiselice <dragostiselice@gmail.com>
- Copyright source: `authors` field

### pest_generator@2.9.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Dragoș Tiselice <dragostiselice@gmail.com>
- Copyright source: `authors` field

### pest_meta@2.9.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Dragoș Tiselice <dragostiselice@gmail.com>
- Copyright source: `authors` field

### pin-project@1.1.13

- SPDX: `Apache-2.0 OR MIT`
- Copyright: no copyright statement in the published crate; SPDX Apache-2.0 OR MIT applies
- Copyright source: neither; the fact is stated

### pin-project-internal@1.1.13

- SPDX: `Apache-2.0 OR MIT`
- Copyright: no copyright statement in the published crate; SPDX Apache-2.0 OR MIT applies
- Copyright source: neither; the fact is stated

### pin-project-lite@0.2.17

- SPDX: `Apache-2.0 OR MIT`
- Copyright: no copyright statement in the published crate; SPDX Apache-2.0 OR MIT applies
- Copyright source: neither; the fact is stated

### pkcs8@0.10.2

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2020-2023 The RustCrypto Project Developers
- Copyright source: licence file

### pkg-config@0.3.34

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### potential_utf@0.1.6

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### ppv-lite86@0.2.21

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2019 The CryptoCorrosion Contributors
- Copyright source: licence file

### primeorder@0.13.6

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2020-2023 RustCrypto Developers
- Copyright source: licence file

### proc-macro-crate@3.5.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Bastian Köcher <git@kchr.de>
- Copyright source: `authors` field

### proc-macro2@1.0.107

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>, Alex Crichton <alex@alexcrichton.com>
- Copyright source: `authors` field

### quick-xml@0.41.0

- SPDX: `MIT`
- Copyright: Copyright (c) 2016 Johann Tuffe
- Copyright source: licence file

### quinn@0.11.11

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018 The quinn Developers
- Copyright source: licence file

### quinn-proto@0.11.17

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018 The quinn Developers
- Copyright source: licence file

### quinn-udp@0.5.15

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018 The quinn Developers
- Copyright source: licence file

### quote@1.0.47

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### r-efi@5.3.0

- SPDX: `MIT OR Apache-2.0 OR LGPL-2.1-or-later`
- Copyright: no copyright statement in the published crate; SPDX MIT OR Apache-2.0 OR LGPL-2.1-or-later applies
- Copyright source: neither; the fact is stated

### r-efi@6.0.0

- SPDX: `MIT OR Apache-2.0 OR LGPL-2.1-or-later`
- Copyright: no copyright statement in the published crate; SPDX MIT OR Apache-2.0 OR LGPL-2.1-or-later applies
- Copyright source: neither; the fact is stated

### rand@0.10.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2018 Developers of the Rand project
- Copyright source: licence file

### rand@0.9.5

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2018 Developers of the Rand project
- Copyright source: licence file

### rand_chacha@0.9.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2018 Developers of the Rand project
- Copyright source: licence file

### rand_core@0.10.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018-2026 The Rand Project Developers
- Copyright source: licence file

### rand_core@0.6.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2018 Developers of the Rand project
- Copyright source: licence file

### rand_core@0.9.5

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2018 Developers of the Rand project
- Copyright source: licence file

### rand_pcg@0.10.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014-2017 Melissa O'Neill and PCG Project contributors
- Copyright source: licence file

### rdkafka@0.36.2

- SPDX: `MIT`
- Copyright: Copyright (c) 2016 Federico Giraud
- Copyright source: licence file

### rdkafka-sys@4.10.0+2.12.1

- SPDX: `MIT`
- Copyright: Federico Giraud <giraud.federico@gmail.com>
- Copyright source: `authors` field

### redox_syscall@0.5.18

- SPDX: `MIT`
- Copyright: Copyright (c) 2017 Redox OS Developers
- Copyright source: licence file

### regex@1.13.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 The Rust Project Developers
- Copyright source: licence file

### regex-automata@0.4.18

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 The Rust Project Developers
- Copyright source: licence file

### regex-syntax@0.8.11

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 The Rust Project Developers
- Copyright source: licence file

### reqwest@0.13.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2016 Sean McArthur
- Copyright source: licence file

### rfc6979@0.4.0

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright 2018-2022 RustCrypto Developers
- Copyright source: licence file

### ring@0.17.14

- SPDX: `Apache-2.0 AND ISC`
- Copyright: Copyright (c) 2009 The Go Authors. All rights reserved.
- Copyright source: licence file

### rustc-hash@2.1.3

- SPDX: `Apache-2.0 OR MIT`
- Copyright: The Rust Project Developers
- Copyright source: `authors` field

### rustc_version@0.4.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 The Rust Project Developers
- Copyright source: licence file

### rustix@1.1.4

- SPDX: `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT`
- Copyright: Dan Gohman <dev@sunfishcode.online>, Jakub Konka <kubkon@jakubkonka.com>
- Copyright source: `authors` field

### rustls@0.23.43

- SPDX: `Apache-2.0 OR ISC OR MIT`
- Copyright: Copyright (c) 2016, Joseph Birr-Pixton <jpixton@gmail.com>
- Copyright source: licence file

### rustls-native-certs@0.8.4

- SPDX: `Apache-2.0 OR ISC OR MIT`
- Copyright: Copyright (c) 2016, Joseph Birr-Pixton <jpixton@gmail.com>
- Copyright source: licence file

### rustls-pki-types@1.15.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2023 Dirkjan Ochtman
- Copyright source: licence file

### rustls-platform-verifier@0.7.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2022 1Password
- Copyright source: licence file

### rustls-platform-verifier-android@0.1.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: no copyright statement in the published crate; SPDX MIT OR Apache-2.0 applies
- Copyright source: neither; the fact is stated

### rustls-webpki@0.103.15

- SPDX: `ISC`
- Copyright: Copyright 2015 Brian Smith.
- Copyright source: licence file

### rustversion@1.0.23

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### ryu@1.0.23

- SPDX: `Apache-2.0 OR BSL-1.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### same-file@1.0.6

- SPDX: `Unlicense/MIT`
- Copyright: Copyright (c) 2017 Andrew Gallant
- Copyright source: licence file

### sasl2-sys@0.1.22+2.1.28

- SPDX: `Apache-2.0`
- Copyright: Materialize, Inc.
- Copyright source: `authors` field

### schannel@0.1.29

- SPDX: `MIT`
- Copyright: Copyright (c) 2015 steffengy
- Copyright source: licence file

### schemars@0.8.22

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Graham Esau
- Copyright source: licence file

### schemars_derive@0.8.22

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Graham Esau
- Copyright source: licence file

### scopeguard@1.2.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016-2019 Ulrik Sverdrup "bluss" and scopeguard developers
- Copyright source: licence file

### sec1@0.7.3

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2021-2022 The RustCrypto Project Developers
- Copyright source: licence file

### secrecy@0.10.3

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2019-2024 iqlusion
- Copyright source: licence file

### security-framework@3.7.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015 Steven Fackler
- Copyright source: licence file

### security-framework-sys@2.17.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015 Steven Fackler
- Copyright source: licence file

### semver@1.0.28

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### serde@1.0.229

- SPDX: `MIT OR Apache-2.0`
- Copyright: Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### serde-value@0.7.0

- SPDX: `MIT`
- Copyright: arcnmx
- Copyright source: `authors` field

### serde_core@1.0.229

- SPDX: `MIT OR Apache-2.0`
- Copyright: Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### serde_derive@1.0.229

- SPDX: `MIT OR Apache-2.0`
- Copyright: Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### serde_derive_internals@0.29.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### serde_json@1.0.151

- SPDX: `MIT OR Apache-2.0`
- Copyright: Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### serde_urlencoded@0.7.1

- SPDX: `MIT/Apache-2.0`
- Copyright: Copyright (c) 2016 Anthony Ramine
- Copyright source: licence file

### serde_yaml@0.9.34+deprecated

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### sha1@0.10.7

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2006-2009 Graydon Hoare
- Copyright source: licence file

### sha2@0.10.9

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2006-2009 Graydon Hoare
- Copyright source: licence file

### sharded-slab@0.1.7

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Eliza Weisman
- Copyright source: licence file

### shared_child@1.1.2

- SPDX: `MIT`
- Copyright: no copyright statement in the published crate; SPDX MIT applies
- Copyright source: neither; the fact is stated

### shlex@1.3.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2015 Nicholas Allegra (comex).
- Copyright source: licence file

### shlex@2.0.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2015 Nicholas Allegra (comex).
- Copyright source: licence file

### sigchld@0.2.5

- SPDX: `MIT`
- Copyright: Jack O'Connor
- Copyright source: `authors` field

### signal-hook@0.4.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2017 tokio-jsonrpc developers
- Copyright source: licence file

### signal-hook-registry@1.4.8

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2017 tokio-jsonrpc developers
- Copyright source: licence file

### signature@2.2.0

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2018-2023 RustCrypto Developers
- Copyright source: licence file

### simd-adler32@0.3.10

- SPDX: `MIT`
- Copyright: Copyright (c) [2021] [Marvin Countryman]
- Copyright source: licence file

### simd_cesu8@1.2.0

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Sean C. Roach <me@seancroach.dev>
- Copyright source: `authors` field

### simdutf8@0.1.5

- SPDX: `MIT OR Apache-2.0`
- Copyright: Hans Kratz <hans@appfour.com>
- Copyright source: `authors` field

### similar@2.7.0

- SPDX: `Apache-2.0`
- Copyright: Armin Ronacher <armin.ronacher@active-4.com>, Pierre-Étienne Meunier <pe@pijul.org>, Brandon Williams <bwilliams.eng@gmail.com>
- Copyright source: `authors` field

### slab@0.4.12

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Carl Lerche
- Copyright source: licence file

### smallvec@1.16.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2018 The Servo Project Developers
- Copyright source: licence file

### socket2@0.6.5

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### spin@0.10.1

- SPDX: `MIT`
- Copyright: Copyright (c) 2014 Mathijs van de Nes
- Copyright source: licence file

### spki@0.7.3

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2021-2023 The RustCrypto Project Developers
- Copyright source: licence file

### stable_deref_trait@1.2.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2017 Robert Grosse
- Copyright source: licence file

### strsim@0.11.1

- SPDX: `MIT`
- Copyright: Copyright (c) 2015 Danny Guo
- Copyright source: licence file

### subtle@2.6.1

- SPDX: `BSD-3-Clause`
- Copyright: Copyright (c) 2016-2017 Isis Agora Lovecruft, Henry de Valence. All rights reserved.
- Copyright source: licence file

### syn@2.0.119

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### syn@3.0.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### sync_wrapper@1.0.2

- SPDX: `Apache-2.0`
- Copyright: Actyx AG <developer@actyx.io>
- Copyright source: `authors` field

### synstructure@0.13.2

- SPDX: `MIT`
- Copyright: Copyright 2016 Nika Layzell
- Copyright source: licence file

### tempfile@3.27.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015 Steven Allen
- Copyright source: licence file

### thiserror@1.0.69

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### thiserror@2.0.20

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### thiserror-impl@1.0.69

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### thiserror-impl@2.0.20

- SPDX: `MIT OR Apache-2.0`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### thread_local@1.1.10

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 The Rust Project Developers
- Copyright source: licence file

### tinystr@0.8.4

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### tinyvec@1.13.2

- SPDX: `Zlib OR Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2019 Daniel "Lokathor" Gee.
- Copyright source: licence file

### tinyvec_macros@0.1.1

- SPDX: `MIT OR Apache-2.0 OR Zlib`
- Copyright: Copyright 2020 Tomasz "Soveu" Marx
- Copyright source: licence file

### tokio@1.53.1

- SPDX: `MIT`
- Copyright: Copyright (c) Tokio Contributors
- Copyright source: licence file

### tokio-macros@2.7.2

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Yoshua Wuyts
- Copyright source: licence file

### tokio-rustls@0.26.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2017 quininer kel
- Copyright source: licence file

### tokio-util@0.7.19

- SPDX: `MIT`
- Copyright: Copyright (c) Tokio Contributors
- Copyright source: licence file

### toml_datetime@1.1.1+spec-1.1.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### toml_edit@0.25.13+spec-1.1.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### toml_parser@1.1.3+spec-1.1.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Individual contributors
- Copyright source: licence file

### tower@0.5.3

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Tower Contributors
- Copyright source: licence file

### tower-http@0.6.11

- SPDX: `MIT`
- Copyright: Copyright (c) 2019-2021 Tower Contributors
- Copyright source: licence file

### tower-layer@0.3.3

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Tower Contributors
- Copyright source: licence file

### tower-service@0.3.3

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Tower Contributors
- Copyright source: licence file

### tracing@0.1.44

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Tokio Contributors
- Copyright source: licence file

### tracing-attributes@0.1.31

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Tokio Contributors
- Copyright source: licence file

### tracing-core@0.1.36

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Tokio Contributors
- Copyright source: licence file

### tracing-log@0.2.0

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Tokio Contributors
- Copyright source: licence file

### tracing-serde@0.2.0

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Tokio Contributors
- Copyright source: licence file

### tracing-subscriber@0.3.23

- SPDX: `MIT`
- Copyright: Copyright (c) 2019 Tokio Contributors
- Copyright source: licence file

### try-lock@0.2.5

- SPDX: `MIT`
- Copyright: Copyright (c) 2018-2023 Sean McArthur
- Copyright source: licence file

### twox-hash@2.1.4

- SPDX: `MIT`
- Copyright: Copyright (c) 2015 Jake Goulding
- Copyright source: licence file

### typenum@1.20.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2014 Paho Lurie-Gregg
- Copyright source: licence file

### ucd-trie@0.1.7

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2015 Andrew Gallant
- Copyright source: licence file

### ulid@1.2.1

- SPDX: `MIT`
- Copyright: Copyright (c) 2017 Dylan Hart
- Copyright source: licence file

### unicode-ident@1.0.24

- SPDX: `(MIT OR Apache-2.0) AND Unicode-3.0`
- Copyright: Copyright © 1991-2023 Unicode, Inc.
- Copyright source: licence file

### unsafe-libyaml@0.2.11

- SPDX: `MIT`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### untrusted@0.9.0

- SPDX: `ISC`
- Copyright: Brian Smith <brian@briansmith.org>
- Copyright source: `authors` field

### ureq@2.12.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2019 Martin Algesten
- Copyright source: licence file

### url@2.5.8

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2013-2025 The rust-url developers
- Copyright source: licence file

### utf8_iter@1.0.4

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Henri Sivonen <hsivonen@hsivonen.fi>
- Copyright source: `authors` field

### utf8parse@0.2.2

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2016 Joe Wilm
- Copyright source: licence file

### valuable@0.1.1

- SPDX: `MIT`
- Copyright: no copyright statement in the published crate; SPDX MIT applies
- Copyright source: neither; the fact is stated

### vcpkg@0.2.15

- SPDX: `MIT/Apache-2.0`
- Copyright: Copyright (c) 2017 Jim McGrath
- Copyright source: licence file

### version_check@0.9.5

- SPDX: `MIT/Apache-2.0`
- Copyright: Copyright (c) 2017-2018 Sergio Benitez
- Copyright source: licence file

### walkdir@2.5.0

- SPDX: `Unlicense/MIT`
- Copyright: Copyright (c) 2015 Andrew Gallant
- Copyright source: licence file

### want@0.3.1

- SPDX: `MIT`
- Copyright: Copyright (c) 2018-2019 Sean McArthur
- Copyright source: licence file

### wasi@0.11.1+wasi-snapshot-preview1

- SPDX: `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT`
- Copyright: The Cranelift Project Developers
- Copyright source: `authors` field

### wasip2@1.0.0+wasi-0.2.4

- SPDX: `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT`
- Copyright: no copyright statement in the published crate; SPDX Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT applies
- Copyright source: neither; the fact is stated

### wasm-bindgen@0.2.127

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### wasm-bindgen-futures@0.4.77

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### wasm-bindgen-macro@0.2.127

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### wasm-bindgen-macro-support@0.2.127

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### wasm-bindgen-shared@0.2.127

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### wasm-streams@0.5.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Mattias Buelens <mattias@buelens.com>
- Copyright source: `authors` field

### web-sys@0.3.104

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2014 Alex Crichton
- Copyright source: licence file

### web-time@1.1.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright 2023 dAxpeDDa
- Copyright source: licence file

### webpki-root-certs@1.0.9

- SPDX: `CDLA-Permissive-2.0`
- Copyright: no copyright statement in the published crate; SPDX CDLA-Permissive-2.0 applies
- Copyright source: neither; the fact is stated

### webpki-roots@0.26.11

- SPDX: `CDLA-Permissive-2.0`
- Copyright: no copyright statement in the published crate; SPDX CDLA-Permissive-2.0 applies
- Copyright source: neither; the fact is stated

### webpki-roots@1.0.9

- SPDX: `CDLA-Permissive-2.0`
- Copyright: no copyright statement in the published crate; SPDX CDLA-Permissive-2.0 applies
- Copyright source: neither; the fact is stated

### weirkeeper@0.1.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2026 The Logweir Authors
- Copyright source: this workspace

### winapi-util@0.1.11

- SPDX: `Unlicense OR MIT`
- Copyright: Copyright (c) 2017 Andrew Gallant
- Copyright source: licence file

### windows-core@0.62.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows-implement@0.60.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows-interface@0.59.3

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows-link@0.2.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows-result@0.4.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows-strings@0.5.1

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows-sys@0.52.0

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows-sys@0.61.2

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows-targets@0.52.6

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows_aarch64_gnullvm@0.52.6

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows_aarch64_msvc@0.52.6

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows_i686_gnu@0.52.6

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows_i686_gnullvm@0.52.6

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows_i686_msvc@0.52.6

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows_x86_64_gnu@0.52.6

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows_x86_64_gnullvm@0.52.6

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### windows_x86_64_msvc@0.52.6

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) Microsoft Corporation.
- Copyright source: licence file

### winnow@1.0.4

- SPDX: `MIT`
- Copyright: no copyright statement in the published crate; SPDX MIT applies
- Copyright source: neither; the fact is stated

### wit-bindgen@0.45.1

- SPDX: `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT`
- Copyright: Alex Crichton <alex@alexcrichton.com>
- Copyright source: `authors` field

### writeable@0.6.4

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### xtask@0.1.0

- SPDX: `Apache-2.0`
- Copyright: Copyright 2026 The Logweir Authors
- Copyright source: this workspace

### yoke@0.8.3

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### yoke-derive@0.8.2

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### zerocopy@0.8.56

- SPDX: `BSD-2-Clause OR Apache-2.0 OR MIT`
- Copyright: Copyright 2023 The Fuchsia Authors
- Copyright source: licence file

### zerocopy-derive@0.8.56

- SPDX: `BSD-2-Clause OR Apache-2.0 OR MIT`
- Copyright: Copyright 2023 The Fuchsia Authors
- Copyright source: licence file

### zerofrom@0.1.8

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### zerofrom-derive@0.1.7

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### zeroize@1.8.2

- SPDX: `Apache-2.0 OR MIT`
- Copyright: Copyright (c) 2018-2021 The RustCrypto Project Developers
- Copyright source: licence file

### zerotrie@0.2.5

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### zerovec@0.11.8

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### zerovec-derive@0.11.6

- SPDX: `Unicode-3.0`
- Copyright: Copyright © 2020-2024 Unicode, Inc.
- Copyright source: licence file

### zlib-rs@0.6.7

- SPDX: `Zlib`
- Copyright: no copyright statement in the published crate; SPDX Zlib applies
- Copyright source: neither; the fact is stated

### zmij@1.0.23

- SPDX: `MIT`
- Copyright: David Tolnay <dtolnay@gmail.com>
- Copyright source: `authors` field

### zstd@0.13.3

- SPDX: `MIT`
- Copyright: Copyright (c) 2016 Alexandre Bury
- Copyright source: licence file

### zstd-safe@7.2.4

- SPDX: `MIT OR Apache-2.0`
- Copyright: Copyright (c) 2016 Alexandre Bury
- Copyright source: licence file

### zstd-sys@2.0.16+zstd.1.5.7

- SPDX: `MIT/Apache-2.0`
- Copyright: Copyright (c) 2016-present, Facebook, Inc. All rights reserved.
- Copyright source: licence file

---

Documentation is licensed [CC-BY-4.0](docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
