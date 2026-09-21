// The pages' runtime: where the run API, the assets and the other pages
// live, and how to authenticate to the API. Whoever serves a page injects
// the JSON in <script id="specimux-runtime"> (see web/pages.py); locally
// everything defaults to the page's own origin and no session is needed.
//
// Every API call the pages make goes through SpecimuxRuntime.fetch /
// eventSource / apiUrl, never a relative path, so a host that serves the
// pages on its own origin can point them at a run API elsewhere.
//
// Session protocol (only when tokenEndpoint and sessionEndpoint are set):
// the page obtains a short-lived run token and exchanges it at
// sessionEndpoint (POST, Bearer token, credentials included) for a run API
// session cookie. Where the token comes from depends on tokenEndpoint:
//
// - same-origin: fetched as JSON ({"token", "expires_in"}) with the page's
//   own cookies, and re-fetched before it expires or when the API answers
//   401 (a host that proxies the page onto its origin);
// - cross-origin: the page navigates there (top level, with a `return`
//   parameter naming this page), the host authorizes the user and comes
//   back to the return URL with `#token=...` in the fragment, which the page
//   exchanges and strips from history. A 401 later sends the page back the
//   same way. A run API that serves its own pages uses this form.
//
// Plain script, no module: pages load it first.
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
  let leaving = false;         // navigating to the authorize URL: stop here

  function sameOrigin(url) {
    try { return new URL(url, window.location.href).origin === window.location.origin; }
    catch (e) { return false; }
  }

  // A token handed over in the URL fragment by the host's authorize route.
  // Taken once, and removed from the address bar and history at once.
  let fragmentToken = null;
  (function takeFragmentToken() {
    const m = /(?:^#|&)token=([^&]+)/.exec(window.location.hash || '');
    if (!m) return;
    fragmentToken = decodeURIComponent(m[1]);
    const rest = (window.location.hash || '').replace(/(?:^#|&)token=[^&]+/, '').replace(/^#&/, '#');
    try {
      window.history.replaceState(null, '', window.location.pathname + window.location.search + (rest === '#' ? '' : rest));
    } catch (e) { /* history unavailable: the fragment stays, harmlessly */ }
  })();

  // Send the browser to the host's authorize URL, which comes back to this
  // page with a fresh token. Guarded so a host that keeps handing out
  // tokens the API refuses cannot bounce the page forever.
  const BOUNCE_KEY = 'specimux-authorize-bounce';
  function navigateToAuthorize() {
    let last = 0;
    try { last = Number(window.sessionStorage.getItem(BOUNCE_KEY) || 0); } catch (e) { /* no storage */ }
    if (Date.now() - last < 10000) {
      throw new Error('authorization bounced back without a usable token');
    }
    try { window.sessionStorage.setItem(BOUNCE_KEY, String(Date.now())); } catch (e) { /* no storage */ }
    const u = new URL(rt.tokenEndpoint, window.location.href);
    u.searchParams.set('return', window.location.href.split('#')[0]);
    leaving = true;
    window.location.assign(u.toString());
    return new Promise(() => {});   // the page is leaving; nothing resolves
  }

  async function postSession(token) {
    const resp = await fetch(rt.sessionEndpoint, {
      method: 'POST',
      credentials: 'include',
      headers: { 'Authorization': `Bearer ${token}` },
    });
    if (!resp.ok) throw new Error(`session endpoint: ${resp.status}`);
    let body = {};
    try { body = await resp.json(); } catch (e) { body = {}; }
    return body;
  }

  // In the cross-origin form the page may already hold a session cookie
  // (a reload, a second tab), so it asks the API before bouncing: a tiny
  // GET that every viewer serves, 401 meaning "no session".
  async function hasSession() {
    try {
      const resp = await fetch(rt.apiUrl('/api/viewers'), { credentials: 'include' });
      return resp.status !== 401;
    } catch (e) { return true; }   // a network failure is not a missing session
  }

  async function exchange(refresh) {
    if (leaving) return new Promise(() => {});
    if (fragmentToken) {
      const token = fragmentToken;
      fragmentToken = null;
      try {
        await postSession(token);
        return;
      } catch (e) {
        // an expired or foreign token in the fragment: fall through and
        // obtain one the normal way
      }
    }
    if (!sameOrigin(rt.tokenEndpoint)) {
      // First call: trust an existing cookie and let a 401 bring us back
      // here with refresh. Refresh (a 401, an SSE error): confirm the
      // session is really gone before leaving the page.
      if (!refresh || await hasSession()) return;
      return navigateToAuthorize();
    }
    const tokResp = await fetch(rt.tokenEndpoint, { credentials: 'same-origin' });
    if (tokResp.status === 401 || tokResp.status === 403) {
      // the host does not know this browser (no login there): go through
      // the host's own front door and come back with a token
      return navigateToAuthorize();
    }
    if (!tokResp.ok) throw new Error(`token endpoint: ${tokResp.status}`);
    const tok = await tokResp.json();
    await postSession(tok.token);
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
    const refresh = !!(opts && opts.refresh);
    if (refresh) sessionPromise = null;
    if (!sessionPromise) {
      sessionPromise = exchange(refresh).catch((e) => { sessionPromise = null; throw e; });
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
