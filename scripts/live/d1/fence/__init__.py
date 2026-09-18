"""D1 §13.1's fenced controller: the isolation the W8 acceptance run lacked.

`fenced` builds and drives a second Logweir controller that lives inside the
test namespace, reaches the API server only through the scoping proxy, and is
the only controller allowed to write there; `rows` is the eight D1 §13.2
scenarios that a shared controller makes unmeasurable.
"""
