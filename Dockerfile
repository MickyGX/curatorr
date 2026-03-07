FROM node:20-alpine

ARG APP_VERSION=""

WORKDIR /app
ENV NODE_ENV=production \
    APP_VERSION=${APP_VERSION}

# better-sqlite3 requires native compilation tools
RUN apk add --no-cache python3 make g++ su-exec

COPY package.json ./
RUN npm install --omit=dev

COPY . .
RUN mkdir -p /app/data /app/config /app/public/icons/custom && chown -R node:node /app
RUN cp /app/config/config.example.json /app/config.example.json

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 7676

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["npm", "start"]
