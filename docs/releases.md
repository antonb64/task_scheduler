# Release process and binary archives

GitHub Actions builds and publishes a release when a branch named `release/vX.Y.Z` is pushed. The branch version must exactly match the version inherited by every Cargo workspace package, and the `Cargo.lock` file must already be current. A version is immutable: after its `vX.Y.Z` tag exists, another push to the same release branch fails instead of replacing the tag or assets.

## Creating a release

Prepare the release on `main`, update the workspace version in `Cargo.toml`, update `Cargo.lock`, and run the normal checks. Then push a release branch pointing at that exact commit:

```sh
git branch release/v0.2.0
git push origin release/v0.2.0
```

The release workflow runs the locked workspace test suite and builds all four binaries before publishing anything. It creates native archives for:

- Linux x86_64 on Ubuntu 22.04 (`x86_64-unknown-linux-gnu`).
- Windows x86_64 with MSVC (`x86_64-pc-windows-msvc`).
- macOS 12 or newer on Intel (`x86_64-apple-darwin`).
- macOS 12 or newer on Apple Silicon (`aarch64-apple-darwin`).

The macOS and Windows archives are not code-signed or notarized. Organizations that require platform signing should add their signing identities in a protected GitHub environment and sign after the tests but before the publish job.

Each archive has one versioned top-level directory containing `coordinator`, `agent`, `task-executor`, and `taskctl`, plus `README.md`, `LICENSE`, `BUILD-METADATA.txt`, the operator documentation, schemas, examples, and deployment scripts. Install binaries from one archive together; coordinator/agent protocol compatibility and the agent/executor contract are versioned in lockstep.

The workflow publishes `SHA256SUMS` alongside the four archives. Verify a downloaded asset before extracting it:

```sh
sha256sum --check SHA256SUMS
```

On macOS, use `shasum -a 256` to compare the value if GNU `sha256sum` is unavailable. On Windows, use `Get-FileHash -Algorithm SHA256`.

The hosted Windows build tests the scheduler and Excel executor code paths that do not need desktop Excel. The licensed, interactive Excel integration suite remains a separate manually dispatched workflow on a self-hosted runner labeled `windows` and `excel`; run it before production promotion when Excel behavior changed.

## Failure behavior

No release is created unless version validation, tests, all platform builds, archive checks, and checksum verification succeed. GitHub's per-run `GITHUB_TOKEN` creates the tag and release, so the release process does not depend on a developer workstation's SSH agent, GitHub CLI session, or commit-signing configuration.

If a build fails, fix the issue on `main`, delete the unpublished remote release branch if necessary, recreate it at the corrected commit, and push it again. Never move a branch after its version tag has been published; increment the version and use a new release branch instead.
