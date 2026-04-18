---
title: Contribution Path
---

# Contribution Path

## Why This Page Exists

New contributors usually do not fail because SeaTunnel lacks extension points. They fail because the entry path is scattered:

- setup is in one page
- connector development is in another page
- community contact points are in README and FAQ
- architecture references are spread across several sections

This page gives a stable onboarding path.

## Who This Page Is For

Use this page if you want to:

- fix a documentation issue
- contribute a connector or transform
- fix a bug
- understand where to ask questions before opening a PR

## Start With the Smallest Valid Entry

Do not begin by trying to understand the whole repository. Start from the contribution type closest to your goal.

### Documentation Contribution

Best first steps:

- fix broken links
- improve quick start wording
- align config docs with real connector options
- add missing English and Chinese documentation together

Start here:

- [Getting Started Overview](../getting-started/overview.md)
- [Job Configuration Guide](../getting-started/job-configuration-guide.md)
- [Docs Format Specification](./docs-format-specification.md)

### Connector Contribution

Best first steps:

- fix one connector option or doc mismatch
- add a small missing capability to an existing connector
- add a new source or sink only after studying one similar connector

Start here:

- [How to Create Your Connector](./how-to-create-your-connector.md)
- [Source Connector Development](./source-connector-development.md)
- [Sink Connector Development](./sink-connector-development.md)

### Transform Contribution

Best first steps:

- improve one existing transform option or example
- fix one focused schema or CDC-related transform behavior
- add a new transform only after studying a similar implementation

Start here:

- [Contribute Transform-V2 Plugins](./contribute-transform-v2-guide.md)
- [Transform Plugin System](../architecture/transform-plugin-system.md)
- [Transforms Catalog](../transforms)

### Code or Architecture Contribution

Best first steps:

- reproduce a specific bug
- add a focused test
- study the smallest relevant module before editing the engine

Start here:

- [Set Up Develop Environment](./setup.md)
- [Architecture Overview](../architecture/overview.md)
- [Core API Design](../architecture/core-api-design.md)

## Recommended Contribution Flow

For most contributors, the shortest safe path is:

1. read the user-facing docs for the feature you want to change
2. reproduce the current behavior locally
3. find one similar implementation in the repository
4. make the smallest change that solves one problem
5. update `docs/en` and `docs/zh` if users will notice the change

This is usually better than starting with a broad refactor.

## Where to Ask Questions

Use these channels depending on the kind of question:

- [GitHub Issues](https://github.com/apache/seatunnel/issues) for concrete bugs, proposals, and tracking
- [dev mailing list](https://lists.apache.org/list.html?dev@seatunnel.apache.org) for longer design discussions and project-wide decisions

If you are unsure where to ask, start with an issue describing the concrete problem and what you already checked.

## What Maintainers Usually Need From You

A contribution is much easier to review when it includes:

- a clear problem statement
- the smallest affected scope
- exact config names and examples
- tests or a clear reason why tests are not practical
- matching documentation updates in English and Chinese

For code contributions, avoid mixing unrelated cleanup with the real fix.

## Good First Contribution Shapes

These contribution shapes tend to land faster:

- improve one doc page and sync `en` + `zh`
- fix one connector option validation issue
- add one missing example or error message
- add one focused unit or E2E test for an existing bug

These contribution shapes tend to need more context:

- changing engine scheduling behavior
- large connector refactors
- changing public config names or defaults

## Contribution Roles in Practice

In day-to-day project work, the most relevant progression is simple:

- users report issues and gaps
- contributors submit fixes and improvements
- long-term contributors may later become more involved in reviews and project direction

For a new contributor, the important point is not the formal role name. It is whether your change is clear, focused, and easy to verify.

## Recommended Reading Path

Pick one path based on your goal:

- docs path: [Docs Format Specification](./docs-format-specification.md) -> [Getting Started Overview](../getting-started/overview.md)
- connector path: [How to Create Your Connector](./how-to-create-your-connector.md) -> [Source Connector Development](./source-connector-development.md) or [Sink Connector Development](./sink-connector-development.md)
- transform path: [Contribute Transform-V2 Plugins](./contribute-transform-v2-guide.md) -> [Transform Plugin System](../architecture/transform-plugin-system.md)
- engine path: [Set Up Develop Environment](./setup.md) -> [Architecture Overview](../architecture/overview.md)
