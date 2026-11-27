# Contributing

Thanks for considering a contribution. You need Go 1.26 or newer.

```
go test -race -short ./...     # the fast run, what CI runs on every push
go test -race ./conformance/...  # the full conformance suites
gofmt -l .                     # must print nothing
go vet ./...
GOOS=darwin go vet ./...       # darwin has to keep building
```

The conformance suites are the contract. `conformance/` checks a Warren against
`docs/service-api.md`, and `conformance/transport` checks a transport against
`docs/transport-api.md`. A change to behaviour that either document describes
changes the document and the suite with it, and a test keeps the numbers in
the documents equal to the code. Within v1 the APIs are additive.

Keep to the existing style: plain Go with comments that say why. Write prose
without em dashes. Report security problems as described in SECURITY.md, not as
issues.
