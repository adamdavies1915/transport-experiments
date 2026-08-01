# glibc base (node:20-slim, not alpine): the @duckdb/node-bindings musl
# packages omit a libc field, so npm can't reliably pick them on Alpine and
# the native binding fails to load. glibc bindings work out of the box.
FROM node:20-slim

WORKDIR /app

# Copy package files and install dependencies
COPY --chown=node:node package*.json ./
RUN npm ci --omit=dev

# Copy source code
COPY --chown=node:node tsconfig.json ./
COPY --chown=node:node src ./src

# Run as the built-in non-root node user
USER node

# Run the TypeScript entrypoint directly via tsx (bundled in dependencies)
CMD ["npm", "start"]
