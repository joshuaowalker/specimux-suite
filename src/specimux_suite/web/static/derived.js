// Shared derived-decision logic for the specimux-suite web UIs.
//
// This is the single JS home for every "decision" the pages compute from
// event state: top-match selection, NS/LQ/chimera routing, taxonomy
// agreement, target status, and the scheduler mirrors (reprocessBand /
// reprocessAssessment). index.html and present.html load it as a plain
// <script>; the parity harness require()s this same file under node and
// checks the scheduler mirrors against scheduler.py (confidence_band and
// reprocess_assessment — keep those in sync with the functions here).
//
// Everything in this file is a pure function of its arguments — no DOM,
// no page globals. Pages bind their own state (genusLineages,
// summarizeFilter, reprocessRatio) via thin adapters.
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.SpecimuxDerived = factory();
})(typeof self !== 'undefined' ? self : this, function () {

// Ranks at or below genus: agreement here is effectively on-target even
// when the name strings differ (iNat's tree often nests an outdated
// binomial under the current genus). Ranks between order and genus are
// "near": a different genus in the same family/tribe — often a taxonomy
// split (Cortinarius→Phlegmacium) rather than a wrong specimen.
const GENUS_OR_DEEPER_RANKS = new Set(['genus', 'subgenus', 'section', 'subsection', 'complex', 'species', 'subspecies', 'variety', 'form', 'hybrid']);
const NEAR_RANKS = new Set(['order', 'suborder', 'infraorder', 'superfamily', 'epifamily', 'family', 'subfamily', 'supertribe', 'tribe', 'subtribe']);

function hasIdentification(s) {
  return (s.identification || []).some(m => m.top_hits && m.top_hits.length > 0);
}

function hitIdentity(h) { return h ? (h.adjusted_identity || h.identity || 0) : 0; }

function hitGenusLower(h) {
  return (h && (h.name || h.ref_id) || '').split(/\s+/)[0].toLowerCase();
}

function isHitOnTarget(hit, communityGenus) {
  if (!communityGenus || !hit) return false;
  return hitGenusLower(hit) === communityGenus;
}

// Lower-cased community genus: resolved genus from iNat ancestors when
// available (infrageneric field IDs), else first word of the community taxon.
function communityGenusLower(s) {
  return (s.community_genus || (s.community_taxon || '').split(/\s+/)[0] || '').toLowerCase();
}

// Preview which summarize track a cluster would be routed to, replicating
// speconsense-summarize's load_consensus_sequences logic. Routing priority:
// lq (err_factor too high) > chimera (core's two-parent recombinant flag,
// only when --filter-chimeras is enabled) > ns (CER not significant). A
// filter value of 0 disables that filter; a null factor (anchor /
// always-pass / pre-0.8.x) always passes. Returns 'lq', 'chimera', 'ns',
// or null (passes through to Summary).
function clusterFilterRouting(cluster, summarizeFilter) {
  const f = summarizeFilter || {};
  const minCer = f.min_cer_factor != null ? f.min_cer_factor : 1.0;
  const maxErr = f.max_err_factor != null ? f.max_err_factor : 1.5;
  if (maxErr > 0 && cluster.err_factor != null && cluster.err_factor > maxErr) return 'lq';
  if (f.filter_chimeras && cluster.chimera) return 'chimera';
  if (minCer > 0 && cluster.cer_factor != null && cluster.cer_factor < minCer) return 'ns';
  return null;
}

// The sequence set a display context works over.
// context: 'clusters' = raw cluster IDs (Processing tab),
// 'variants' = variant IDs with cluster fallback (Summary tab).
function activeSeqs(s, context) {
  if ((context || 'clusters') === 'variants' && s.variants && s.variants.length) {
    return s.variants;
  }
  return s.clusters || [];
}

function getActiveMatches(s, context) {
  const seqs = activeSeqs(s, context);
  const seqNames = new Set(seqs.map(seq => seq.name));
  return (s.identification || []).filter(m => m.top_hits && m.top_hits.length && (!seqs.length || seqNames.has(m.cluster)));
}

function findTopMatch(s, seqs, communityGenus, summarizeFilter) {
  const seqSizeMap = {};
  const seqByName = {};
  for (const seq of seqs) { seqSizeMap[seq.name] = seq.size || 0; seqByName[seq.name] = seq; }

  // Candidate matches in the active sequence set, preferring unflagged
  // sequences: an NS/LQ/chimera cluster's hit only represents the specimen
  // when nothing clean has a hit. (Matters most under identity ranking —
  // flagged clusters are small, so size ranking rarely surfaced them.)
  let candidates = [];
  const flagged = [];
  for (const match of (s.identification || [])) {
    if (!match.top_hits || !match.top_hits.length) continue;
    if (seqs.length && !(match.cluster in seqSizeMap)) continue;
    const seq = seqByName[match.cluster];
    if (seq && clusterFilterRouting(seq, summarizeFilter)) flagged.push(match);
    else candidates.push(match);
  }
  if (!candidates.length) candidates = flagged;

  // Rank: on-target first; among on-target sequences show the best
  // identity achieved (a small 100% cluster beats a large 96% one);
  // among off-target-only, keep size so junk micro-clusters with a
  // lucky match can't displace the dominant signal.
  let bestMatch = null;
  let bestOnTarget = false;
  let bestSize = -1;
  let bestIdentity = -1;
  for (const match of candidates) {
    const top = match.top_hits[0];
    const onTarget = isHitOnTarget(top, communityGenus);
    const size = seqSizeMap[match.cluster] || 0;
    const identity = hitIdentity(top);
    if (bestMatch === null
        || (onTarget && !bestOnTarget)
        || (onTarget === bestOnTarget
            && (onTarget ? identity > bestIdentity : size > bestSize))) {
      bestMatch = match;
      bestOnTarget = onTarget;
      bestSize = size;
      bestIdentity = identity;
    }
  }
  return bestMatch;
}

function getTopMatch(s, context, summarizeFilter) {
  if (!s.identification || !s.identification.length) return null;
  return findTopMatch(s, activeSeqs(s, context), communityGenusLower(s), summarizeFilter);
}

function getTopHit(s, context, summarizeFilter) {
  const m = getTopMatch(s, context, summarizeFilter);
  return m ? m.top_hits[0] : null;
}

// Genus-string on/off-target: does any hit in the active set match the
// community genus? Null when there is no field ID to compare against.
function getTargetStatus(s, context) {
  if (!s.community_taxon) return null;
  const communityGenus = communityGenusLower(s);
  for (const m of getActiveMatches(s, context)) {
    for (const hit of m.top_hits) {
      if (hitGenusLower(hit) === communityGenus) return 'on-target';
    }
  }
  return 'off-target';
}

// Deepest taxonomic level at which the field ID and the displayed top hit
// agree: {level: 'species'|'genus'|<iNat rank>, name} or null when unknown
// (no field ID, no hit, or lineage data not yet fetched). Display-only —
// scheduler banding stays genus-based.
function agreementRank(s, hit, genusLineages) {
  if (!hit || !s.community_taxon) return null;
  const hitName = (hit.name || hit.ref_id || '').trim();
  if (hitName.toLowerCase() === s.community_taxon.trim().toLowerCase()) {
    return { level: 'species', name: hitName };
  }
  const communityGenus = communityGenusLower(s);
  const hitGenusName = hitName.split(/\s+/)[0];
  if (hitGenusName.toLowerCase() === communityGenus) {
    return { level: 'genus', name: hitGenusName };
  }
  const lineage = (genusLineages || {})[hitGenusName.toLowerCase()];
  if (!lineage || !lineage.length || !(s.inat_ancestors || []).length) return null;
  const anc = new Set(s.inat_ancestors);
  for (let i = lineage.length - 1; i >= 0; i--) {
    if (anc.has(lineage[i].id)) return { level: lineage[i].rank, name: lineage[i].name };
  }
  return null;
}

// The one place genus-string on/off-target is upgraded with taxonomy-level
// agreement. Both the ✓/≈/✗ indicator and the On-/Off-target filter chips
// consume this, so a specimen can never show ✓ yet sit in the Off-target
// filter. Returns 'on-target' | 'on-target-taxonomy' | 'near' | 'off-target'
// | null. (Scheduler banding — reprocessBand — stays genus-based.)
// env: {genusLineages, summarizeFilter}.
function effectiveTargetStatus(s, context, env) {
  const t = getTargetStatus(s, context);
  if (t !== 'off-target') return t;
  const a = agreementRank(s, getTopHit(s, context, env && env.summarizeFilter), env && env.genusLineages);
  if (a && GENUS_OR_DEEPER_RANKS.has(a.level)) return 'on-target-taxonomy';
  if (a && NEAR_RANKS.has(a.level)) return 'near';
  return 'off-target';
}

// Mirror of scheduler.confidence_band (scheduler.py) — keep in sync
// (parity-tested against a replayed event log by tests/test_mirror_parity.py).
// Band 1 = most worth revisiting with more reads. Reason tokens are
// identical to the Python side; pages map them to display labels.
function reprocessBand(s) {
  const ident = s.identification || [];
  if (!ident.some(m => (m.top_hits || []).length)) {
    if (s.status === 'no_match') return { band: 1, reason: 'no_match' };
    return { band: 4, reason: 'pending' };
  }
  const communityGenus = communityGenusLower(s);
  const sizes = {};
  for (const c of (s.clusters || [])) sizes[c.name] = c.size || 0;
  const hasSizes = (s.clusters || []).length > 0;
  let bestHit = null, bestOn = false, bestSize = -1;
  let domHit = null, domSize = -1;
  let onAnywhere = false;
  for (const m of ident) {
    if (!(m.top_hits || []).length) continue;
    if (hasSizes && !(m.cluster in sizes)) continue;
    const top = m.top_hits[0];
    const size = sizes[m.cluster] || 0;
    if (size > domSize) { domSize = size; domHit = top; }
    if (communityGenus && m.top_hits.some(h => hitGenusLower(h) === communityGenus)) onAnywhere = true;
    const on = !!communityGenus && hitGenusLower(top) === communityGenus;
    if (bestHit === null || (on && !bestOn) || (on === bestOn && size > bestSize)) {
      bestHit = top; bestOn = on; bestSize = size;
    }
  }
  if (!bestHit) return { band: 4, reason: 'pending' };
  const identity = hitIdentity(bestHit);
  if (identity < 0.90) return { band: 2, reason: 'low_identity' };
  if (communityGenus) {
    if (!onAnywhere) return { band: 3, reason: 'off_target' };
    if (domHit && hitGenusLower(domHit) !== communityGenus) return { band: 3, reason: 'minority_on_target' };
  }
  let domAmbig = 0, domChimera = null, domClusterSize = -1;
  for (const c of (s.clusters || [])) {
    if ((c.size || 0) > domClusterSize) {
      domClusterSize = c.size || 0; domAmbig = c.ambig || 0; domChimera = c.chimera || null;
    }
  }
  if (identity < 0.98 || domAmbig > 0 || domChimera) return { band: 4, reason: 'marginal' };
  return { band: 5, reason: 'confident' };
}

// Mirror of scheduler.reprocess_assessment (scheduler.py) — keep in sync
// (parity-tested). Uncertain results (bands 1-3) re-enter the queue at half
// the configured reprocess_ratio, and reprocessing always needs >=5 new
// reads (MIN_NEW_READS_FOR_REPROCESS). Returns {eligible, ratio, band,
// reason}; band 0 = not banded (never processed / gated out / no clusters).
function reprocessAssessment(s, reprocessRatio) {
  if ((s.consensus_version || 0) === 0) return { eligible: false, ratio: 0, band: 0, reason: 'never_processed' };
  if (s.status === 'consensus_running') return { eligible: false, ratio: 0, band: 0, reason: 'running' };
  const base = s.reads_at_last_consensus || 0;
  const newReads = (s.total_reads || 0) - base;
  if (newReads < 5) return { eligible: false, ratio: 0, band: 0, reason: 'too_few_new_reads' };
  const ratio = base > 0 ? newReads / base : Infinity;
  if (!(s.clusters || []).length) {
    return { eligible: ratio > reprocessRatio, ratio, band: 0, reason: 'no_clusters' };
  }
  const b = reprocessBand(s);
  const gate = b.band <= 3 ? reprocessRatio / 2 : reprocessRatio;
  return { eligible: ratio > gate, ratio, band: b.band, reason: b.reason };
}

return {
  GENUS_OR_DEEPER_RANKS, NEAR_RANKS,
  hasIdentification, hitIdentity, hitGenusLower, isHitOnTarget,
  communityGenusLower, clusterFilterRouting, activeSeqs, getActiveMatches,
  findTopMatch, getTopMatch, getTopHit, getTargetStatus, agreementRank,
  effectiveTargetStatus, reprocessBand, reprocessAssessment,
};
});
