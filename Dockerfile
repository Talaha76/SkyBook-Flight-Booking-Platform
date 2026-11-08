# One image for every SkyBook service; docker-compose picks the entrypoint per service.
FROM node:22-bookworm-slim

WORKDIR /app
ENV NPM_CONFIG_UPDATE_NOTIFIER=false KAFKAJS_NO_PARTITIONER_WARNING=1

COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY tsconfig.json jest.config.js ./
COPY packages ./packages
COPY services ./services
COPY scripts ./scripts
COPY tests ./tests
RUN npx tsc -p tsconfig.json

USER node
EXPOSE 8080 3001 3002 3003 3004 3005
CMD ["node", "dist/services/gateway/src/index.js"]
