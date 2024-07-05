/**
 * Use this to serve the parquetjs bundle at http://localhost:8000/main.js
 * It attaches the parquet.js exports to a "parquetjs" global variable.
 * See the example server for how to use it.
 */
const { compressionBrowserPlugin, wasmPlugin, resolveBrowserFieldPlugin } = require('./esbuild-plugins');
// esbuild has TypeScript support by default. It will use .tsconfig
require('esbuild')
  .context({
    entryPoints: ['parquet.ts'],
    outfile: 'main.js',
    define: { 'process.env.NODE_DEBUG': 'false', 'process.env.NODE_ENV': '"production"', global: 'window' },
    platform: 'browser',
    plugins: [compressionBrowserPlugin, wasmPlugin, resolveBrowserFieldPlugin],
    sourcemap: 'external',
    bundle: true,
    minify: false,
    globalName: 'parquetjs',
    inject: ['./esbuild-shims.js'],
  })
  .then((context) => {
    context
      .serve({
        servedir: __dirname,
      })
      .then((server) => {
        console.log('serving parquetjs', server);
      });
  });
