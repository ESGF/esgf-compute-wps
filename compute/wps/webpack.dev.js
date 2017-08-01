var webpackMerge = require('webpack-merge');
var commonConfig = require('./webpack.common.js');
var helpers = require('./helpers');

module.exports = webpackMerge(commonConfig, {
  output: {
    path: helpers.root('assets/js'),
    filename: '[name]-[hash].js',
  }
});
