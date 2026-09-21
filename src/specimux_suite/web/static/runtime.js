// The pages' runtime: where the run API, the assets and the other pages
// live, and how to authenticate to the API. Whoever serves a page injects
// the JSON in <script id="specimux-runtime"> (see web/pages.py); locally
// everything defaults to the page's own origin and no session is needed.
//
// Every API call the pages make goes through SpecimuxRuntime.fetch /
// eventSource / apiUrl, never a relative path, so a host that serves the
// pages on its own origin can point them at a run API elsewhere.
//
// Session protocol (only when tokenEndpoint is set): the page fetches a
// run token from its own origin (tokenEndpoint, with the page's cookies),
// exchanges it at sessionEndpoint (Bearer token, credentials included) for
// a run API cookie, and repeats before the token expires or when the API
// answers 401. Plain script, no module: pages load it first.
(function () {
  'use strict';

  const tag = document.getElementById('specimux-runtime');
  let cfg = {};
  try { cfg = tag ? JSON.parse(tag.textContent || '{}') : {}; } catch (e) { cfg = {}; }

  const rt = {
    apiBase: (cfg.apiBase || '').replace(/\/$/, ''),
    assetBase: (cfg.assetBase || '').replace(/\/$/, ''),
    pageBase: (cfg.pageBase || '').replace(/\/$/, ''),
    tokenEndpoint: cfg.tokenEndpoint || null,
    sessionEndpoint: cfg.sessionEndpoint || null,
  };

  rt.apiUrl = (path) => rt.apiBase + path;
  rt.pageUrl = (path) => rt.pageBase + path;

  // --- session ---
  let sessionPromise = null;   // in-flight exchange, shared by all callers
  let refreshTimer = null;

  async function exchange() {
    const tokResp = await fetch(rt.tokenEndpoint, { credentials: 'same-origin' });
    if (!tokResp.ok) throw new Error(`token endpoint: ${tokResp.status}`);
    const tok = await tokResp.json();
    const sessResp = await fetch(rt.sessionEndpoint, {
      method: 'POST',
      credentials: 'include',
      headers: { 'Authorization': `Bearer ${tok.token}` },
    });
    if (!sessResp.ok) throw new Error(`session endpoint: ${sessResp.status}`);
    // Re-exchange at 80% of the token's life (default 10 minutes).
    const ttl = Number(tok.expires_in) > 0 ? Number(tok.expires_in) : 600;
    clearTimeout(refreshTimer);
    refreshTimer = setTimeout(() => rt.ready({ refresh: true }).catch(() => {}), ttl * 800);
  }

  // Resolves once the page may call the API. Without a token endpoint
  // there is nothing to do. With one, the first call performs the
  // exchange; {refresh: true} forces a new one (expiry, a 401).
  rt.ready = (opts) => {
    if (!rt.tokenEndpoint || !rt.sessionEndpoint) return Promise.resolve();
    if (opts && opts.refresh) sessionPromise = null;
    if (!sessionPromise) {
      sessionPromise = exchange().catch((e) => { sessionPromise = null; throw e; });
    }
    return sessionPromise;
  };

  // fetch against the API: credentials always (harmless same-origin; the
  // run API cookie cross-origin), one re-exchange-and-retry on a 401.
  rt.fetch = async (path, init) => {
    await rt.ready();
    const doFetch = () => fetch(rt.apiUrl(path), { credentials: 'include', ...(init || {}) });
    let resp = await doFetch();
    if (resp.status === 401 && rt.tokenEndpoint) {
      await rt.ready({ refresh: true });
      resp = await doFetch();
    }
    return resp;
  };

  rt.eventSource = (path) => new EventSource(rt.apiUrl(path), { withCredentials: true });

  window.SpecimuxRuntime = rt;
})();
