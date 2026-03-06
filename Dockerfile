FROM node:20-alpine

ARG APP_VERSION=""

WORKDIR /app
ENV NODE_ENV=production \
    APP_VERSION=${APP_VERSION}

COPY package.json ./
RUN npm install --omit=dev

RUN apk add --no-cache su-exec

COPY . .
RUN mkdir -p /app/data /app/config /app/public/icons/custom && chown -R node:node /app
RUN cp /app/config/default-apps.json /app/default-apps.json
RUN cp /app/config/default-categories.json /app/default-categories.json
RUN cp /app/config/config.example.json /app/config.example.json
RUN chmod 644 /app/default-apps.json /app/default-categories.json /app/config/default-apps.json /app/config/default-categories.json

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 3333

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["npm", "start"]
