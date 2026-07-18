# Contribute Connector-V2 Plugins

If you want to contribute Connector-V2, use the internal docs below as the main entry path, then read the repository guide for module-level details.

## Writing Connector FAQ Sections

When you add or update connector FAQ sections, treat them as a navigation layer, not a second
source of truth.

- Keep exact option names, defaults, and full behavior details in the connector option table and
  detailed sections.
- In FAQ answers, prefer short answers plus links or explicit pointers to the existing sections on
  the same page.
- If a FAQ mentions a connector option, verify the spelling and behavior against the current
  connector doc and code before merging.
- If a point depends on passthrough properties or upstream system behavior, label it explicitly
  instead of presenting it as a standard SeaTunnel connector option.
- Keep English and Chinese FAQ scope aligned.

- [Contribution Path](./contribution-path.md)
- [How to Create Your Connector](./how-to-create-your-connector.md)
- [Source Connector Development](./source-connector-development.md)
- [Sink Connector Development](./sink-connector-development.md)
- [Set Up Develop Environment](./setup.md)
- External repository guide: [Connector-v2 Contribution Guide](../../../seatunnel-connectors-v2/README.md)
