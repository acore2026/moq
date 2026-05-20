# Reference Integrations

This directory stores lightweight integration notes and patches for external
projects. It intentionally does not vendor whole external repositories.

## ACN SDK

Apply `patches/acn_sdk_rust_cli.patch` to an ACN SDK checkout to add the
environment-driven Rust MoQ client switch:

```bash
cd /path/to/acn_sdk
git apply /path/to/moq/py/acn-moq-rust/reference/patches/acn_sdk_rust_cli.patch
```

Then run the SDK with:

```bash
export ACN_MOQ_IMPL=rust-cli
```

The default remains the original Python MoQ client when `ACN_MOQ_IMPL` is not
set.
