# Security Policy

justrade is a spot exchange matching engine. Defects can have financial impact,
so security reports are taken seriously and handled with care.

## Supported versions

The project is pre-1.0 and under active development. Security fixes are applied
to the default branch (`master`) and, where applicable, the most recent tagged
release. There is no long-term support commitment for older tags yet.

| Version        | Supported          |
|----------------|--------------------|
| `master`       | Yes                |
| Latest release | Yes                |
| Older releases | No                 |

## Reporting a vulnerability

Please do not open a public issue, pull request, or discussion for security
problems.

Report privately using one of the following:

- GitHub private vulnerability reporting: open the repository's "Security" tab
  and choose "Report a vulnerability" (preferred).
- Email: brian.pham.ptt@gmail.com.

Include as much of the following as you can:

- A description of the issue and its impact (for example, incorrect settlement,
  value leakage, a determinism break, a denial-of-service vector, or a way to
  double-apply a command).
- Steps to reproduce, ideally a minimal test or a recorded command sequence.
- Affected module(s) and version or commit hash.
- Any suggested remediation.

## What to expect

- Acknowledgment of your report within a few business days.
- An initial assessment and severity classification.
- Coordinated disclosure: we will agree on a timeline before any public
  discussion, and credit you in the release notes if you wish.

## Scope

In scope:

- The matching engine and settlement logic (`core`).
- Consensus, journaling, and replay integrity (`launcher`, `read`).
- Wire codecs and message handling (`protocol`).
- Client SDKs and the gateway (`write-client`, `read-client`, `gateway`).

Out of scope:

- Vulnerabilities in third-party dependencies (report those upstream; we will
  bump versions once fixes are available).
- Findings that require a misconfigured or non-default, insecure deployment
  contrary to the documented guidance.
- Theoretical issues without a demonstrable impact on correctness, determinism,
  availability, or confidentiality.

Thank you for helping keep justrade and its users safe.
