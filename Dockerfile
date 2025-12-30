FROM node:20-alpine

WORKDIR /app

# Create non-root user first
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001

# Copy package files and install dependencies
COPY --chown=nodejs:nodejs package*.json ./
RUN npm ci --omit=dev

# Copy source code
COPY --chown=nodejs:nodejs src ./src

# Switch to non-root user
USER nodejs

CMD ["node", "src/index.js"]
