# seatunnel-engine-ui

## Development Environment Dependencies

- Node 18+/20+ required
- pnpm (or npm 7+)

```sh
npm install -g pnpm
```

- modify `VITE_APP_API_SERVICE` and `VITE_APP_API_BASE` in `.env.development`
- quick start

```sh
pnpm install
pnpm dev
```

## Project Setup

```sh
pnpm install
```

### Compile and Hot-Reload for Development

```sh
pnpm dev
```

### Type-Check, Compile and Minify for Production

```sh
pnpm build
```

### Run Unit Tests with [Vitest]

```sh
pnpm test:unit
```

### Run End-to-End Tests with [Cypress]

```sh
pnpm test:e2e:dev
```

This runs the end-to-end tests against the Vite development server.
It is much faster than the production build.

But it's still recommended to test the production build with `test:e2e` before deploying (e.g. in CI environments):

```sh
pnpm build
pnpm test:e2e
```

### Lint with [ESLint]

```sh
pnpm lint
```
