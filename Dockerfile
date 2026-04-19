FROM node:20-alpine

ARG APP_VERSION=""

WORKDIR /app
ENV NODE_ENV=production \
    APP_VERSION=${APP_VERSION}

# better-sqlite3 requires native compilation tools, su-exec handles privilege drop,
# wget keeps existing compose healthchecks working unchanged, and tzdata allows
# TZ to drive SQLite/localtime and server-side date formatting reliably.
RUN apk add --no-cache python3 make g++ su-exec wget tzdata

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY . .
RUN mkdir -p /app/data /app/config /app/public/icons/custom && chown -R node:node /app

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 7676

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["npm", "start"]
