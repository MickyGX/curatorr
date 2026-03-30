FROM node:20-bookworm-slim

ARG APP_VERSION=""

WORKDIR /app
ENV NODE_ENV=production \
    APP_VERSION=${APP_VERSION}

# better-sqlite3 requires native compilation tools, and gosu handles privilege drop.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 make g++ gosu \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY . .
RUN mkdir -p /app/data /app/config /app/public/icons/custom && chown -R node:node /app

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 7676

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["npm", "start"]
