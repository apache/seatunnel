# FilterRowKind

> FilterRowKind transform plugin

## Description

Filter the data by RowKind

## Options

| name          | type  | required | default value |
|---------------|-------| -------- |---------------|
| include_kinds | array | yes      |               |
| exclude_kinds | array | yes      |               |

### include_kinds [array]

The row kinds to include

### exclude_kinds [array]

The row kinds to exclude.

You can only config one of `include_kinds` and `exclude_kinds`.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details
