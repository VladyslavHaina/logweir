"""Extra fixtures a single D1 row needs, kept out of the common `fixture.py`.

`fixture.py` is what every scenario shares: one PLAINTEXT broker, one MinIO,
one destination. A fixture that exists for ONE row — an ACL-enforcing broker,
for instance — does not belong there, because every other row would then pay
for its startup and its credential.
"""
