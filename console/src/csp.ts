// The console runs under logweir-api's Content-Security-Policy, which has no
// 'unsafe-eval'. Zod probes `new Function` once to decide on its fast path; under
// that policy the probe is reported as a violation even though Zod catches it.
// `jitless` skips the probe. Imported first by main.tsx.
import { z } from "zod";

z.config({ jitless: true });
