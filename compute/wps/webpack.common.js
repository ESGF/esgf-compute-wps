var webpack = require('webpack');
var BundleTracker = require('webpack-bundle-tracker');
var helpers = require('./helpers');

module.exports = {
  entry: {
    'polyfills': './assets/polyfills.ts',
    'vendor': './assets/vendor.ts',
    'app': './assets/main.ts'
  },

  resolve: {
    extensions: ['.ts', '.js']
  },

  module: {
    rules: [
      {
        test: /\.ts$/,
        loaders: [
          {
            loader: 'awesome-typescript-loader',
            options: { configFileName: helpers.root('wps', 'assets', 'tsconfig.json') }
          }, 'angular2-template-loader'
        ]
      },
      {
        test: /\.html$/,
        loader: 'html-loader'
      },
      {
        test: /\.css$/,
        include: helpers.root('wps', 'assets', 'app'),
        loader: 'raw-loader'
      },
      { 
        test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        loader: "url-loader?limit=10000&mimetype=application/font-woff" 
      },
      { 
        test: /\.(ttf|eot|svg|woff(2)?)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        loader: "file-loader?outputPath=static/js/"
      }
    ]
  },

  plugins: [
    new BundleTracker({ filename: './webpack-stats.json' }),

    new webpack.ContextReplacementPlugin(
      /angular(\\|\/)core(\\|\/)@angular/,
      helpers.root('./wps/assets'),
      {}
    ),

    new webpack.optimize.CommonsChunkPlugin({
      name: ['app', 'vendor', 'polyfills']
    })
  ],

  node: {
    fs: 'empty'
  }
};
