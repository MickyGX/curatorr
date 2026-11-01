FROM node:24-alpine

ARG APP_VERSION=""
# Fail the build if the base image resolves to a Node release older than this
# (24.22.1 patches CVE-2026-111497, see #187).
ARG MIN_NODE_VERSION=24.22.1

RUN node -e "const want=process.argv[1].split('.').map(Number),have=process.versions.node.split('.').map(Number);for(let i=0;i<3;i++){if(have[i]>want[i])process.exit(0);if(have[i]<want[i]){console.error('Node '+process.versions.node+' is older than required '+process.argv[1]);process.exit(1)}}" "${MIN_NODE_VERSION}"

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
