# Replace

> Replace transform plugin

## Description

Examines string value in a given field and replaces substring of the string value that matches the given string literal or regexes with the given replacement.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- |---------------|
| replace_field  | string | no       |               |
| pattern        | string | yes      | -             |
| replacement    | string | yes      | -             |
| is_regex       | boolean| no       | false         |
| replace_first  | boolean| no       | false         |

### source_field [string]

The field you want to replace

### field [string]

The name of the field to replaced.

### pattern [string]

The string to match.

### replacement [string]

The replacement pattern (is_regex is true) or string literal (is_regex is false).

### is_regex [boolean]

Whether or not to interpret the pattern as a regex (true) or string literal (false).

### replace_first [boolean]

Whether or not to skip any matches beyond the first match.

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details