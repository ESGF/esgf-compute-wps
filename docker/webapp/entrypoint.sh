#! /bin/bash

./node_modules/.bin/light-server -s ./assets/ -p 80 -w "src/**/*.ts,src/**/*.html,src/**/*.css # ./node_modules/.bin/webpack --config config/webpack.dev.js # reload" --historyindex "/index.html"
