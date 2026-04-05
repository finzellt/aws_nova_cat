## Testing

When running tests, ALWAYS exclude smoke tests:

```
pytest --ignore=tests/smoke -q
```

Never run `pytest` without `--ignore=tests/smoke`. Smoke tests hit live AWS
infrastructure and are only for manual validation, not for code change verification.

This matches the CI configuration in `.github/workflows/ci.yml`.
