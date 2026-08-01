# glibc base (node:20-slim, not alpine): the @duckdb/node-bindings musl
# packages omit a libc field, so npm can't reliably pick them on Alpine and
# the native binding fails to load. glibc bindings work out of the box.
FROM node:20-slim

# ca-certificates: the MotherDuck extension connects over gRPC/TLS and needs a
# CA root bundle (slim ships none), else it fails with "Could not get default
# pem root certs". GRPC_DEFAULT_SSL_ROOTS_FILE_PATH points gRPC at that bundle.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt

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
