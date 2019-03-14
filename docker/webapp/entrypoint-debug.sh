#! /bin/bash

./node_modules/.bin/http-server ./assets/ -p 80 >/dev/null 2>&1 &

./node_modules/.bin/webpack --config config/webpack.dev.js --watch
