# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's private vulnerability
reporting: the **Security** tab of
[PeterSR/warren-mesh](https://github.com/PeterSR/warren-mesh), then *Report a
vulnerability*. Do not open a public issue for anything you believe is a
security problem.

You can expect an acknowledgement within a few days. A reproduction helps
most: a call, an entry or a request that Warren should have refused.

## Scope

What Warren promises, and what it leaves to the deployment, is written down in
the threat model of [docs/architecture.md](docs/architecture.md#threat-model).
Reports about behaviour the threat model names as out of scope (for example,
anything that can reach a service socket being that service) are welcome as
suggestions but are not vulnerabilities.

## Supported versions

Security fixes target the latest 0.x release only.
