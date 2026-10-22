# =============================================================================
# Catastro Spain Module — Frontend IIFE Bundle Build
# =============================================================================
# Builds the module as a self-contained IIFE bundle (nkz-module.js).
# In production, this is served as a static file from MinIO, not via nginx.
# This Docker image is only used for backwards-compat CI/CD; the actual
# deployment uploads dist/nkz-module.js directly to MinIO.
# =============================================================================

# Stage 1: Build with Vite
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files (the repo lockfile is pnpm-lock.yaml)
COPY package.json pnpm-lock.yaml ./

# Reproducible install from the committed lockfile (pnpm version pinned by
# "packageManager" in package.json). package.json has no file: dependencies.
RUN corepack enable && pnpm install --frozen-lockfile

# Copy source files needed for building
COPY src/ ./src/
COPY index.html ./
COPY vite.config.ts ./
COPY tsconfig.json ./
COPY tsconfig.node.json ./
COPY tailwind.config.js ./
COPY postcss.config.js ./
COPY manifest.json ./

# Build the IIFE bundle
RUN pnpm run build

# Stage 2: Lightweight image with just the bundle
FROM nginx:alpine

# Copy built bundle
COPY --from=builder /app/dist /usr/share/nginx/html/modules/catastro-spain

# Copy nginx config (serves the module bundle)
COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
