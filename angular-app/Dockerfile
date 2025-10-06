# Build stage
FROM node:20-alpine AS build

WORKDIR /app

# Copy package files first for better caching
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy Angular configuration files
COPY angular.json ./
COPY tsconfig*.json ./

# Create assets directory
RUN mkdir -p src/assets

# Copy source code
COPY src/ ./src/

# Build the Angular application
RUN npm run build

# Production stage
FROM nginx:alpine

# Remove default nginx website
RUN rm -rf /usr/share/nginx/html/*

# Copy built Angular application from the build stage
COPY --from=build /app/dist/angular-app /usr/share/nginx/html

# Copy custom nginx configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Create a simple test page to verify nginx is working
RUN echo '<h1>Angular App Container is Running</h1><p>If you see this, the container is working but Angular build may have failed.</p>' > /usr/share/nginx/html/test.html

# Expose port 80
EXPOSE 80

# Start nginx
CMD ["nginx", "-g", "daemon off;"]