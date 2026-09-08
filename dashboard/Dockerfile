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

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install production dependencies only
RUN npm install --omit=dev

# Copy built frontend from builder
COPY --from=builder /app/dist ./dist

# Copy the saved-summary server (run via tsx)
COPY server-motherduck.ts ./
COPY server ./server
COPY tsconfig.node.json ./
COPY src ./src

RUN mkdir -p /app/data
ENV TRANSIT_SUMMARY_CACHE_FILE=/app/data/transit-summary.json
VOLUME ["/app/data"]

# Expose port
EXPOSE 3000

# Start server
CMD ["npm", "start"]
