# Repository instructions

Keep documentation current in the same change as the behavior it describes:

- `README.md`: project overview, quick start, architecture, and build prerequisites.
- `USAGE.md`: detailed application usage, public APIs, configuration, and examples.
- `VENDORS.md`: external dependencies, versions, maintainers, and purposes; distinguish
  direct dependencies from dependencies that remain transitively.

Changes to usage, configuration, public APIs, builds, or dependencies must update
the relevant documents above. Describe verified behavior. Do not make cosmetic
edits to documents that are unaffected.

Preserve persisted data and protocol compatibility unless a migration is explicitly
part of the task. Builds must remain offline and use vendored sources. After
changing inventoried source files or dependency pins, regenerate the inventory with
`python3 third-party/verify.py --regenerate` and verify it with
`python3 third-party/verify.py`.

Keep unrelated working-tree changes intact. Validate Rust changes with the affected
offline Cargo tests and native-link changes with CMake integration and installed
consumer tests. Use `MURMUR_BUILD_DIR` to select the native CMake build tree for
Cargo tests when needed.

For small work choose internal code over adding external dependency, if external dependdency
need to be added choose one that is supported by a corporation like aws, google, microsoft,
meta, etc. first; library can be either c, cpp or rust, choose rust first if supported by 
a corporation.  The goal is to have a low amount of external depedencies.
