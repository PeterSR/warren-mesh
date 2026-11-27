# Releasing

A release is a `v*` tag. Pushing one runs [.github/workflows/release.yml](.github/workflows/release.yml),
which first runs the checks of `test.yml` on the tagged commit and publishes nothing if they fail.
It then builds with goreleaser ([.goreleaser.yaml](.goreleaser.yaml)) and creates a **draft** GitHub
release with:

- archives for Linux on amd64 and arm64, each with `warren`, `warren-tailscale`, `warren-update`,
  `warren-release`, `LICENSE`, `README.md`, `THIRD_PARTY_NOTICES`, `examples/warren.yaml`,
  `examples/update.yaml` and `contrib/warren.service`;
- an SPDX SBOM for each archive;
- `checksums.txt`, signed with cosign keyless (`checksums.txt.sigstore.json`);
- notes taken from the version's `CHANGELOG.md` section;
- a build provenance attestation for the archives.

Signing is keyless: cosign signs with a short-lived certificate for the workflow's GitHub OIDC
identity, recorded in the public Sigstore transparency log. There are no keys to keep. Only Linux
is released; darwin builds but is not supported.

## Cutting a release

1. Make sure `main` is green.
2. Set `Version` in `internal/version/version.go` to the new version. The binaries report it, and
   the workflow fails the release when it differs from the tag without its `v`. A release
   candidate sets it to, for example, `"0.1.0-rc.1"` and is tagged `v0.1.0-rc.1`.
3. In `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`, add a new empty
   `## [Unreleased]` above it, and update the links at the bottom. The release fails when the
   version has no section; check with `scripts/changelog-section.sh vX.Y.Z`. (Before the first
   release the section is already `## [0.1.0] - Unreleased`: put the date in its place, and add
   the links at the bottom once the tags exist.)
4. Commit and get it onto `main`, then tag the commit and push the tag:

   ```bash
   git tag -s vX.Y.Z -m vX.Y.Z
   git push origin vX.Y.Z
   ```

5. When the workflow is green, open the draft release, read it, and publish it.

A tag with a `-` suffix (`v0.2.0-rc.1`) makes a prerelease. Tags must follow Semantic Versioning
without build metadata; the workflow refuses anything else.

To try the release build locally, without signing or publishing, run
`goreleaser release --snapshot --clean --skip=sign` (it needs goreleaser and syft); it writes to
`dist/`, which is ignored.

A failed release can be retried by rerunning the workflow: goreleaser reuses the existing draft and
replaces its assets. Once a release is published, do not rerun it or move the tag: fix what was
wrong and release the next patch version.

## Repository settings

Set these once: a tag ruleset for `refs/tags/v*` so only maintainers can create, update or delete
release tags; and the default `GITHUB_TOKEN` permission can stay read-only, since the workflow asks
for what each job needs.

## Verifying a release

You need [cosign](https://docs.sigstore.dev/cosign/system_config/installation/) 3.1 or newer. The
signature names the release workflow on a version tag of this repository. Set `VERSION` to the
release you are checking:

```bash
VERSION=X.Y.Z
IDENTITY='^https://github\.com/PeterSR/warren-mesh/\.github/workflows/release\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'
OIDC=https://token.actions.githubusercontent.com

cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp "$IDENTITY" --certificate-oidc-issuer "$OIDC"
sha256sum --ignore-missing -c checksums.txt
```

The build provenance, with the GitHub CLI:

```bash
gh attestation verify "warren_${VERSION}_linux_amd64.tar.gz" --repo PeterSR/warren-mesh
```

To accept only one release, replace the regular expression with that tag's exact identity:
`--certificate-identity "https://github.com/PeterSR/warren-mesh/.github/workflows/release.yml@refs/tags/v$VERSION"`.
