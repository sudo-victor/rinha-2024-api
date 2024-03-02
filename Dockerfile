FROM node:20-alpine

WORKDIR /app

RUN apk add --no-cache libc6-compat openssl-dev

COPY package.json yarn.lock ./

RUN yarn --production

COPY . .

EXPOSE 3000

# Comando para rodar o aplicativo
CMD ["yarn", "start"]
