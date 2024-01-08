FROM node:16.15.0-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci

COPY . .

RUN ["npx", "@11ty/eleventy"]

ENV PORT=8080

EXPOSE 8080

CMD ["npx", "@11ty/eleventy" "--serve"]