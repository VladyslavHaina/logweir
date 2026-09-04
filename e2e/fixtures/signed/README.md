# Signed test fixtures

Everything in this directory, including `public.pem` and `signing.pem`, is a **throwaway test fixture**: the P-256 key pair here signs nothing outside this repository's own test suite, it is regenerated on demand by `just fixtures-sign`, and it must never be used to sign a real drill scorecard.
