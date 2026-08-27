# seatunnel-engine-ui

## Development Environment Dependencies

- Node 18+/20+ required
- npm 7+

- modify `VITE_APP_API_SERVICE` and `VITE_APP_API_BASE` in `.env.development`
- quick start

```sh
npm install
npm run dev
```

## Project Setup

```sh
npm install
```

### Compile and Hot-Reload for Development

```sh
npm run dev
```

### Type-Check, Compile and Minify for Production

```sh
npm run build
```

### Run Unit Tests with [Vitest]

```sh
npm run test:unit
```

### Run End-to-End Tests with [Cypress]

```sh
npm run test:e2e:dev
```

This runs the end-to-end tests against the Vite development server.
It is much faster than the production build.

But it's still recommended to test the production build with `test:e2e` before deploying (e.g. in CI environments):

```sh
npm run build
npm run test:e2e
```

### Lint with [ESLint]

```sh
npm run lint
```
