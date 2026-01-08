FROM node:20-slim

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy source files
COPY *.js ./

# Start the scheduler
CMD ["npm", "start"]
