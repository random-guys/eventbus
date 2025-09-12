FROM node:lts-alpine3.21 

WORKDIR /app

COPY package.json .

RUN yarn

COPY . .

