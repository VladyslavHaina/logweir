# A package only so pytest can collect e2e/k8s/d2 and e2e/k8s/d3 in ONE session:
# both hold a `test_rows.py`, and two rootless modules of one basename collide.
