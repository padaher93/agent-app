# real_shadow_test partition

Isolated partition for redacted real borrower packages in shadow mode.

Rules:
- Keep this partition physically and logically separated from proxy datasets.
- Evaluate separately from `proxy_test` until sample size and stability thresholds are met.
- Do not mix files between `dataset/packages/*` proxy paths and this partition.
