// Test-side SSR projection only: seed an authorized cache, then a real error state.
// No HTTP/model/database operation and no files under product source are written.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const test = require('node:test');
const web = path.resolve(__dirname, '..');
const fromWeb = createRequire(path.join(web, 'package.json'));
const { buildSync } = fromWeb('esbuild');
const React = fromWeb('react');
const { renderToStaticMarkup } = fromWeb('react-dom/server');
const { QueryClient, QueryClientProvider } = fromWeb('@tanstack/react-query');
const compiled = buildSync({ stdin: { contents: `
  export { CatalogPage } from './src/catalog.tsx';
  export { HistoryPage } from './src/history.tsx';
  export { ApiError } from './src/api.ts';`,
  resolveDir: web, loader: 'ts', sourcefile: 'privacy-probe-entry.ts' },
  bundle: true, write: false, platform: 'node', format: 'cjs', packages: 'external', jsx: 'automatic' });
const loaded = { exports: {} };
new Function('require', 'module', 'exports', compiled.outputFiles[0].text)(fromWeb, loaded, loaded.exports);
const { CatalogPage, HistoryPage, ApiError } = loaded.exports;
const sentinel = 'PRIVATE-CACHED-CONTENT-DO-NOT-RENDER';
const user = { user_id: 'actor-test', username: 'actor', role: 'analyst_east', effective_scope: 'east',
  authz_version: 1, capabilities: ['catalog:read', 'history:read_self', 'queries:create'] };
const api = { get() { throw new Error('NO_NETWORK_IN_PROJECTION_TEST'); } };
function cachedError(client, key, data) {
  client.setQueryData(key, data);
  client.getQueryCache().find({ queryKey: key }).setState({ status: 'error', fetchStatus: 'idle',
    error: new ApiError(403, { code: 'FORBIDDEN', message: 'Current capability was revoked.' }) });
}
function render(client, component, props) {
  return renderToStaticMarkup(React.createElement(QueryClientProvider, { client }, React.createElement(component, props)));
}
function client() { return new QueryClient({ defaultOptions: { queries: { retry: false, retryOnMount: false, gcTime: Infinity } } }); }
const base = ['datapilot', user.user_id, user.authz_version];

test('COM-T042 catalog capability error hides previously cached private definitions', () => {
  const c = client();
  const directory = JSON.parse(fs.readFileSync(path.resolve(web, '../catalog/metrics.v2.yaml'), 'utf8'));
  directory.metrics[0].name = sentinel;
  const envelope = { request_id: 'test-only', catalog_version: directory.catalog_version,
    definition_hash: 'test-only-hash', schema_fingerprint: 'test-only-hash', effective_scope: 'east',
    readiness: { state: 'unknown', code: 'CATALOG_READINESS_UNKNOWN' }, metric_readiness: [] };
  cachedError(c, [...base, 'catalog-metrics', null], { ...envelope, metrics: directory.metrics });
  c.setQueryData([...base, 'catalog-schema', null], { ...envelope, views: {}, join_paths: [], allowed_features: [] });
  assert.doesNotMatch(render(c, CatalogPage, { api, user, version: null, metricId: null }), new RegExp(sentinel));
  c.clear();
});

test('COM-T042 history capability error hides previously cached private questions', () => {
  const c = client();
  cachedError(c, [...base, 'history', ''], { pages: [{ request_id: 'test-only', next_cursor: null,
    items: [{ query_id: 'query-private', question: sentinel, status: 'completed', status_version: 2,
      spec_version: 1, effective_scope: 'east', catalog_version: 'catalog-test', data_version: 'data-test',
      api_received_at: '2026-11-07T00:00:00Z', finished_at: null, current_version_matches: true }] }], pageParams: [null] });
  assert.doesNotMatch(render(c, HistoryPage, { api, user, open() {}, rerun() {} }), new RegExp(sentinel));
  c.clear();
});

test('COM-T042 report capability error hides previously cached administrator report titles', () => {
  const c = client();
  const admin = { ...user, role: 'report_admin', effective_scope: null, capabilities: ['reports:read'] };
  cachedError(c, [...base, 'reports'], { pages: [{ request_id: 'test-only', next_cursor: null,
    items: [{ report_id: 'report-private', title: sentinel, report_type: 'synthetic_test_report', status: 'not_run',
      ready: false, gold_reviewed: false, created_at: '2026-11-07T00:00:00Z' }] }], pageParams: [null] });
  assert.doesNotMatch(render(c, HistoryPage, { api, user: admin, open() {}, rerun() {} }), new RegExp(sentinel));
  c.clear();
});
