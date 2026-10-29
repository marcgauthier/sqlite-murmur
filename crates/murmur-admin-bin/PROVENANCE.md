# murmur-admin-bin provenance

CLI entry-point crate for murmur-admin (in-repo; no archive source).
Provides `main` (argc/argv from the C runtime) and delegates to
`murmur_admin::admin_main`. Built as a staticlib; CMake links it with
`libmurmur_admin.a` into the `murmur-admin` executable.
