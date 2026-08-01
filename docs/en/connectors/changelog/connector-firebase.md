---

## 2. Register Document in Sidebar (`sidebars.js`)

SeaTunnel uses **Docusaurus** to generate its documentation site. You must add your new doc to `docs/sidebars.js` for it to appear in the navigation tree.

Open `docs/sidebars.js` and locate the `connector-v2` section:

```javascript
module.exports = {
    docs: [
        {
            type: 'category',
            label: 'Connector-V2',
            items: [
                {
                    type: 'category',
                    label: 'Source',
                    items: [
                        // ... existing connectors ...
                        'connector-v2/source/Fake',
                        'connector-v2/source/Firebase', // <-- Add your connector here (without .md)
                        'connector-v2/source/JDBC',
],
},
// ...
],
},
],
};