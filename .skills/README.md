# .skills Directory

Store project-local skills in this directory.

Each skill should live in its own subdirectory and include a `SKILL.md` file as the entry point.
Add supporting files only when they are necessary for repeated use, such as:

- `agents/openai.yaml` for UI metadata
- `scripts/` for reusable automation
- `references/` for task-specific guidance
- `assets/` for templates or other output resources

Keep skills focused on repository-specific workflows, conventions, and repeatable tasks.
Avoid adding unrelated notes, temporary files, or general project documentation here.
