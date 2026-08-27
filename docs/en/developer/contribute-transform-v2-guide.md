---
title: Contribute Transform-V2 Plugins
---

# Contribute Transform-V2 Plugins

## Why This Page Exists

Transform contribution is one of the most approachable ways to add user-visible capability to SeaTunnel, but the entry path is easy to miss because the information is split across transform docs, architecture pages, and repository READMEs.

This page gives a shorter entry path.

## Start Here

If you want to contribute a transform plugin, read these pages first:

- [Contribution Path](./contribution-path.md)
- [Set Up Develop Environment](./setup.md)
- [Transform Plugin System](../architecture/transform-plugin-system.md)
- [Transform Common Options](../transforms/common-options/common-options.md)

These pages explain the development environment, where transforms sit in the pipeline, and how transform datasets are connected through `plugin_input` and `plugin_output`.

## Common Contribution Shapes

Most transform-related contributions fall into one of these buckets:

- add a new transform plugin
- add or refine one option in an existing transform
- fix schema, metadata, or CDC-related transform behavior
- improve transform documentation and examples

The smallest safe contribution is usually better than starting with a large transform refactor.

## What To Study Before Editing Code

Before modifying `seatunnel-transforms-v2`, check at least one similar transform implementation and compare:

- factory definition and option validation
- runtime transform contract
- schema or metadata behavior
- unit or integration tests
- matching `docs/en` and `docs/zh` pages

Transforms are easier to review when the code, docs, and examples change together.

## Recommended Reading Path

1. [Transform Plugin System](../architecture/transform-plugin-system.md)
2. [Transforms Catalog](../transforms)
3. [Transform Common Options](../transforms/common-options/common-options.md)
4. [Core API Design](../architecture/core-api-design.md)
5. [seatunnel-transforms-v2 README](../../../seatunnel-transforms-v2/README.md)

## When You Need The Repository-Level Guide

For module-specific details such as transform module conventions or examples inside the `seatunnel-transforms-v2` tree, continue with the repository guide:

- [Transform-V2 Contribution Guide](../../../seatunnel-transforms-v2/README.md)
