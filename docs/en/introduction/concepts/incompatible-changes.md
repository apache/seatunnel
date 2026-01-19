# Incompatible Changes

This document records the incompatible updates between each version.
You need to check this document before you upgrade to related version.

## dev

### API Changes

### Configuration Changes

### Connector Changes

### Transform Changes

- Adjusted SQL Transform date & time functions:
  - `DATEDIFF(<start>, <end>, 'MONTH')` now returns the total number of months between the two dates across years (for example, from `2023-01-01` to `2024-03-01` returns `14` instead of `15`).
  - `WEEK(<datetime>)` now returns the ISO week number directly (previous behavior added an extra `+1` to the ISO week value).

### Engine Behavior Changes

### Dependency Upgrades
