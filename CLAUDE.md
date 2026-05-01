# CLAUDE.md

Guidance for Claude Code in this DuckDB research fork.

---

## 1. Operating Mode (MANDATORY)

**Ask Mode (Read-Only) by default.** Analyze, explain, design, and answer
questions. Deliver code as text in chat, not as file edits, unless
explicitly authorized.

### 1.1 File Modification

- **Read tools always allowed:** `Read`, `Glob`, `Grep`, and non-mutating
  `Bash` (`ls`, `cat`, `rg`, `find`, `git log`, `git diff`, `git status`).
- **Do NOT use** `Edit`, `Write`, `Create`, `MultiEdit`, or mutating
  `Bash` (`sed -i`, `>`, `>>`, `mv`, `rm`, `git commit`, `git add`,
  `make`, `cmake`, `ninja`) unless the user's message contains an
  explicit action verb: `edit`, `fix`, `write`, `refactor`, `patch`,
  `apply`, `create`, `rename`, `delete`, `commit`.
- **No unsolicited fixes.** Bugs noticed during analysis → describe in
  chat, don't queue an edit.
- **No speculative scaffolding.** No stub files, no "helpful" supporting
  artifacts.
- If intent is ambiguous, ask one clarifying question rather than edit.

### 1.2 Workflow

For non-trivial changes, follow a design-first loop:

1. **Locate** — cite relevant files, classes, call sites (paths + line
   ranges).
2. **Design** — describe the change: types/methods touched, invariants,
   control/data flow, alternatives. Reference existing DuckDB patterns.
3. **Present code** — full modified functions/classes as fenced blocks.
   Diffs only on request.
4. **Validate plan** — list tests and pragmas to exercise the change.

---

## 2. Project Context

Research fork focused on **query optimization and query evaluation**.
Active work:

- A new optimizer sub-phase that transforms the `LogicalOperator` tree
  into a some new data structures. And based on the new data structures,
  some new algorithms to adjust on the logical operator tree. 
- New logical/physical operators: **semi-join** and **bloom filter**,
  each with *separated build and probe operators* (not fused).
- May have some runtime optimizations on the physical operators. 
- SIMD acceleration where profitable.

### 2.1 Relevant Source Areas

- `src/optimizer/` — optimizer phases; new sub-phase lives here.
- `src/optimizer/join_order/` — existing join-order infrastructure
  (`JoinOrderOptimizer`, `QueryGraph`, `JoinNode`, DPhyp enumeration).
  **Study before designing the join-tree IR**; their relationship is
  the first design question.
- `src/planner/operator/` and
  `src/include/duckdb/planner/operator/` — logical operators.
- `src/execution/operator/join/` — physical joins;
- `src/execution/physical_plan_generator.cpp` — logical → physical
  lowering dispatch.
- Include paths are case-sensitive on Linux CI even though APFS is not.

### 2.2 DuckDB Idioms

- **Sink lifecycle:** `GetGlobalSinkState` → `GetLocalSinkState` →
  `Sink` → `Combine` → `Finalize`.
- **Source lifecycle:** `GetGlobalSourceState` → `GetLocalSourceState`
  → `GetData`.
- **Operator-in-the-middle:** `GetGlobalOperatorState` →
  `GetOperatorState` → `Execute`.
- **Build/probe separation** requires shared state across pipeline
  boundaries. See recursive CTE / `PhysicalColumnDataScan` for the
  idiomatic mechanism before inventing one.
- **Pipeline dependencies** are expressed via `MetaPipeline` in
  `BuildPipelines`. Probe pipeline must depend on build `Finalize`.
- **New operator types** require enum entries in
  `logical_operator_type.hpp` and `physical_operator_type.hpp` — this
  affects serialized-plan compatibility.
- **Vector discipline:** `DataChunk` up to `STANDARD_VECTOR_SIZE`
  (2048). Allocate per-chunk or per-state, never per-tuple. Use
  `UnifiedVectorFormat` for mixed flat/constant/dictionary input.
- **Hashing:** reuse `VectorOperations::Hash` and
  `src/common/types/hash.cpp`. Do not introduce a new hash function
  without justification.
- **Unbounded state** (hash tables, bloom filters over large builds)
  should route through `BufferManager` eventually. Plain heap is
  acceptable for a prototype if stated as a known limitation.

### 2.3 SIMD

- Scalar fallback is required; SIMD is the optimization path, not the
  only path.
- Target x86-64 (SSE4.2 baseline, AVX2 opportunistic) and ARM64 (NEON).
- Isolate SIMD in leaf functions; operator `Execute`/`Sink` stays
  plain C++.
- No SIMD commit without a microbenchmark and a query-level
  measurement.

### 2.4 Development Environment

- Platform: macOS. `GEN=ninja make debug` for iteration.
- Tests: `build/debug/test/unittest "[optimizer]"`, `"[join]"`,
  `"[join_order]"`, plus any new tags this project introduces.
- Diagnostic pragmas:
  - `PRAGMA enable_verification;`
  - `PRAGMA verify_parallelism;`
  - `PRAGMA explain_output='all';`
  - `PRAGMA disabled_optimizers='...';` to isolate the new sub-phase.
- Sanitizers: `BUILD_UNITTEST=1 ENABLE_SANITIZER=1 GEN=ninja make debug`.

### 2.5 Response Conventions

- Match SIGMOD/VLDB/TODS/PODS/CIDR rigor for theoretical analysis:
  state invariants, complexity, assumptions.
- Cite concrete symbols (`PhysicalHashJoin::BuildPipelines`), not
  generic descriptions.
- Disambiguate operators: "semi-join build operator" vs "semi-join
  probe operator", never "the semi-join".
- Don't fabricate paths or signatures. Grep and report.

---

## 3. What NOT to Do

- No unprompted builds, commits, branches, or PRs.
- No auto-formatting or include reordering in files touched for reading.
- No multi-file patches in chat without agreed design.
- No SIMD without scalar fallback and benchmark plan.
- No duplication of existing DuckDB machinery without stated reason.