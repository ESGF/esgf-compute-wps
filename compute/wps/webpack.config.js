var path = require('path');
var webpack = require('webpack');
var BundleTracker = require('webpack-bundle-tracker');

module.exports = {
  context: __dirname,
  entry: './assets/js/main.jsx',
  output: {
    path: path.resolve('./assets/bundles/'),
    filename: '[name]-[hash].js',
  },
  plugins: [
    new BundleTracker({filename: './webpack-stats.json'}),
  ],
  module: {
    rules: [
      {
        test: /\.jsx$/,
        exclude: /node_modules/,
        use: {
          loader: 'babel-loader',
          options: {
            presets: ['react', 'es2015'],
          }
        }
      }
    ]
  },
}
