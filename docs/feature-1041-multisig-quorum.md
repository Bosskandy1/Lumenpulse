Feature: #1041 — Multi-admin quorum execution for treasury and registry

Summary
- Treasury and Contributor Registry implement multisig proposal lifecycle (propose → sign → execute).
- Configurable signer weights and threshold allow N-of-M quorum configurations.
- Duplicate sign attempts and unauthorized executors are rejected.
- Sensitive actions are gated by `consume_approval` which marks proposals Executed when consumed.

Covered contracts & tests
- apps/onchain/contracts/treasury — multisig.rs, lib.rs, storage.rs, and tests in src/test.rs covering propose, sign, execute, duplicate sign, insufficient quorum, expiry, cancel.
- apps/onchain/contracts/contributor_registry — multisig.rs, lib.rs, storage.rs, tests in src/lib.rs module covering propose/sign/execute and action-specific gating.

Notes
- Implementation already enforces threshold checks, duplicate-approval rejection, action-type matching, and executor authorization.
- Tests include happy path and insufficient quorum scenarios for both contracts.

If you want, I can open a PR or run the onchain contract test suite locally to verify further.
