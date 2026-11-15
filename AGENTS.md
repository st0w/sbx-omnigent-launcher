# Working in sbx-omnigent-launcher

The launcher runs Omnigent agents in Docker Sandbox microVMs and drives
declarative multi-agent pipelines. `README.md` explains what it does;
`docs/PIPELINES.md` explains the pipeline runner.

## Code shape

- Keep every code and test file under 1,000 lines, and aim for 300-350.
  Split by responsibility before a file reaches that, not after.
- Keep every function at McCabe complexity 10 or below (ruff `C901`).
- One responsibility per module. Several files for one responsibility go
  in one package, whose `__init__.py` holds only a docstring; import
  from the defining module.
- Files over the limit when it was adopted are listed in
  `code-shape-baseline.toml`. Never grow one. When a change shrinks one,
  lower its entry in the same change, and remove it once the file is
  within the limit.
- Prefer composition and dependency injection over inheritance and
  mixins.

## Conventions

- Single quotes everywhere, docstrings included. Docstrings and comments
  wrap at 72 characters, code at 79.
- Docstrings say what a function does and why, with `:param:`,
  `:returns:` and `:raises:`. Keep the history behind a decision next to
  the code it explains.
- Never suppress a finding (`noqa`, `type: ignore`, `nosemgrep`): change
  the code so the rule no longer matches, or ask.
- Tests come first and use `unittest`. `tests/` is not a package, so a
  test module never imports another.
- Use uv, never pip. Pin every new dependency to an exact version at
  least seven days old, and update `uv.lock`.
- Documentation never contains real filesystem paths: use placeholders
  like `/path/to/project`.

## Commands

The tests need the environment Omnigent is installed in (see "Running
the tests" in `README.md`):

```sh
/path/to/omnigent-venv/bin/python -W error -m unittest discover -s tests
SKIP=no-commit-to-branch pre-commit run --all-files
uvx mypy@1.19.0 --python-executable /path/to/omnigent-venv/bin/python sbx_omnigent
uvx semgrep scan --config auto --quiet --error
```
