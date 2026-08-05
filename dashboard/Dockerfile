# Build stage
FROM node:20-slim AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install all dependencies (including dev for build)
RUN npm install

# Copy source files
COPY . .

# Build frontend
RUN npm run build

# Production stage
FROM node:20-slim

# ca-certificates: the MotherDuck extension connects over gRPC/TLS and needs a
# CA root bundle (slim ships none), else it fails with "Could not get default
# pem root certs". GRPC_DEFAULT_SSL_ROOTS_FILE_PATH points gRPC at that bundle.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=/etc/ssl/certs/ca-certificates.crt

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install production dependencies only
RUN npm install --omit=dev

# Copy built frontend from builder
COPY --from=builder /app/dist ./dist

# Copy MotherDuck server (run via tsx, bundled in dependencies)
COPY server-motherduck.ts ./
COPY tsconfig.node.json ./
COPY src ./src

# Expose port
EXPOSE 3000

# Start server
CMD ["npm", "start"]
