FROM node:6-stretch as builder

COPY webapp webapp

WORKDIR webapp

RUN yarn && \
      ./node_modules/.bin/webpack --config config/webpack.prod.js

FROM nginx:1.15

WORKDIR /usr/share/nginx/html

COPY --from=builder /assets static/

RUN mv static/js/index.html .
