<!-- Thanks for contributing to justrade. Please fill out this template. -->

## Summary

<!-- What does this change do, and why? Link the issue it addresses. -->

Closes #

## Type of change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that changes existing behavior)
- [ ] Documentation only
- [ ] Build, CI, or tooling

## Hot path impact

- [ ] This change does NOT touch a hot path (`decode`, `onEvent`, `dispatch`, `lookup`, `update`, `encode`, `publish`, `consume`).
- [ ] This change touches a hot path and includes JMH before/after numbers below.

<!-- If hot path: paste JMH results and, where relevant, an allocation check (-prof gc). -->

```
before:
after:
```

## Determinism and correctness

- [ ] No new nondeterminism (no wall-clock reads, randomness, unordered iteration, or floating point in the engine).
- [ ] Settlement value conservation is preserved (including fees).
- [ ] Replay determinism holds (recorded session in produces byte-identical session out), if applicable.

## CI gate

Ran locally, all passing with zero errors and zero warnings:

- [ ] `./gradlew spotlessApply`
- [ ] `./gradlew checkstyleMain checkstyleTest`
- [ ] `./gradlew compileJava`
- [ ] `./gradlew test integrationTest`
- [ ] `python3 scripts/jmh-regression.py` (strict > 10% mean regression vs baseline, advisory tail)
- [ ] `python3 scripts/jmh-regression.py --gc` (zero-allocation contract, hot-path changes)

## Tests

<!-- Describe the tests added or updated (unit, property, integration, cluster, fault). -->

## Documentation

- [ ] Updated relevant docs / module READMEs, or none needed.
- [ ] Added or superseded an ADR under `docs/decisions/`, or not applicable.

## Notes for reviewers

<!-- Anything else reviewers should know. -->
