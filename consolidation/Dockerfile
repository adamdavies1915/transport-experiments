FROM node:20-alpine

WORKDIR /app

# Copy package files
COPY package.json ./

# Install dependencies
RUN npm install --production

# Copy source files
COPY index.js ./
COPY schema.sql ./

# Run the consolidation service
CMD ["npm", "start"]
