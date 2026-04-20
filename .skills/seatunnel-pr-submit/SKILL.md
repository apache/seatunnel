---
name: seatunnel-pr-submit
description: Draft or submit Apache SeaTunnel pull requests in English using the repository's required title and body conventions. Use when Codex needs to prepare a PR from the current branch or diff, infer a compliant `[Type][Module][SubModule] Description` title, fill the required PR template, describe what changed from git history or file diffs, or open the PR after the user provides or omits the change summary.
---

# SeaTunnel PR Submit

## Overview

Draft SeaTunnel PR titles and bodies that match the repository rules.
Prefer the user's own summary of the change, then use the current branch diff to verify or complete it.

## Workflow

### 1. Collect PR context

Inspect the current change set before writing anything.
Prefer committed branch changes when the branch is already ahead of its base.
If the user is preparing a PR before committing, inspect staged and unstaged changes and say that the draft reflects the working tree.
Read the repository's current PR template before drafting the body so the generated content follows the latest sections and checklist wording.

Use non-interactive git commands such as:

```bash
git branch --show-current
git status --short
git diff --stat
git diff --name-only
git diff --cached --stat
git log --oneline -5
sed -n '1,220p' .github/PULL_REQUEST_TEMPLATE.md
```

If an upstream branch exists, also inspect the branch-level change set:

```bash
git log --oneline @{upstream}..HEAD
git diff --stat @{upstream}...HEAD
git diff --name-only @{upstream}...HEAD
```

### 2. Prefer the user's summary

Use the user's description as the primary explanation of intent.
Use the diff only to sharpen wording, identify the correct module, and catch obvious mismatches.
If the user's summary materially conflicts with the diff, pause and call out the mismatch before submitting the PR.

### 3. Infer the PR title

Write the title in English.
Use one of these formats:

- `[Type][Module] Description`
- `[Type][Module][SubModule] Description`

Choose `Type` from the dominant change:

| Signal | Type |
| --- | --- |
| New behavior, new option, new capability | `Feature` |
| Bug fix or correction | `Fix` |
| Optimization, cleanup, or non-breaking refinement | `Improve` |
| Urgent CI, dependency, or release blocker fix | `Hotfix` |
| Documentation-only change | `Docs` |
| Test-only change | `Test` |
| Build, tooling, or maintenance work | `Chore` |

Choose `Module` from the dominant path:

| Path pattern | Module | SubModule hint |
| --- | --- | --- |
| `seatunnel-connectors-v2/...` | `Connector-V2` | Connector name |
| `seatunnel-transforms-v2/...` | `Transform-V2` | Transform name |
| `seatunnel-engine/...` | `Zeta` | Engine area if clear |
| `seatunnel-core/...` | `Core` | Omit when not needed |
| `seatunnel-api/...` | `API` | Omit when not needed |
| `seatunnel-e2e/...` | `E2E` | Suite or connector name |
| `.github/...`, CI workflow files | `CI` | Workflow or job name |
| `bin/...`, shell scripts | `Shell` | Script name |
| `tools/...` | `Tools` | Tool name |
| `docs/...` only | `Docs` | Usually omit submodule |

Apply these rules:

- Prefer a single dominant module instead of listing every touched area.
- Add `SubModule` only when one connector, transform, or area clearly dominates the diff.
- Keep the description concise and action-oriented.
- Describe the actual change, not the development process.
- Do not add Chinese text to the title.

Examples:

- `[Fix][Connector-V2][Milvus] Fix partition creation`
- `[Feature][Transform-V2] Support AES_GCM algorithm in FieldEncrypt`
- `[Improve][Zeta] Optimize multiple table job config parsing`
- `[Docs] Remove redundant lines from transform docs`

### 4. Write the PR body

Mirror the current `.github/PULL_REQUEST_TEMPLATE.md` headings and checklist structure, and keep the body in English.
Use the repository template as the source of truth when wording changes in the future.
At minimum, include these sections:

```md
### Purpose of this pull request
<Explain what changed and why. Prefer 1 short paragraph or 2 flat bullets. Mention the real files, modules, or behaviors changed.>

### Does this PR introduce _any_ user-facing change?
<Write the behavior difference if users can notice it. For docs-only or internal-only changes, write `No`.>

### How was this patch tested?
<List the real verification that ran. Never claim tests that did not run. If nothing ran yet, write `Not run yet.`>

### Check list
- [ ] New dependency is documented under the New License Guide
- [ ] Documentation is updated if needed
- [ ] incompatible-changes.md is updated if needed
- [ ] Connector changes include plugin-mapping.properties, seatunnel-dist pom, CI label, E2E, and plugin_config updates if needed
```

Write the body from evidence:

- Mention the exact scope of the diff instead of generic phrases.
- If the user already summarized the change, use that wording as the backbone of the first section.
- If the user gave no summary, infer the purpose from changed files, removed or added lines, and recent commits.
- For docs-only changes, explain which docs were updated and what was corrected or removed.
- Keep the same checklist intent as the repository template even if individual links or wording have changed.
- Leave checklist items unchecked by default unless the current evidence clearly supports checking them.

### 5. Handle testing honestly

Before claiming a PR is ready, prefer repository validation that matches the actual scope.
For SeaTunnel work, the common baseline is:

```bash
./mvnw spotless:apply
./mvnw -q -DskipTests verify
./mvnw test
```

Use narrower verification when the change is local and the user prefers a smaller check, but never invent coverage.
If validation has not happened yet, say so directly in the PR body and recommend the relevant command before opening the PR.

### 6. Submit the PR when asked

If the user asks to open the PR, prepare the final title and body first, then create the PR with a non-interactive flow.
Prefer the available GitHub integration.
If you fall back to `gh`, use a non-interactive command such as `gh pr create --title ... --body-file ...`.

Keep these constraints:

- Follow the repository title format exactly.
- Keep the PR body in English.
- Describe only the current branch change set.
- Do not mention other assistants or tool brands in the PR title or body.
- Do not claim tests, docs, or compatibility work that did not happen.

## Output Format

When the user asks for a draft, return:

```md
Title: [Type][Module][SubModule] Description

Body:
### Purpose of this pull request
...

### Does this PR introduce _any_ user-facing change?
...

### How was this patch tested?
...

### Check list
- [ ] ...
```

When the user also gives a manual summary, integrate it first and only use the diff to refine the title and fill in missing facts.
