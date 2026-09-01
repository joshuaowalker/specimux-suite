#!/usr/bin/env node
// Node side of the mirror-parity tests (tests/test_mirror_parity.py).
//
// Reads {derived_path, specimens, reprocess_ratios} as JSON on stdin, runs
// the PRODUCTION derived.js (the same file the pages load) over each
// specimen snapshot, and prints the decisions as JSON. The Python test
// computes the same decisions from scheduler.py and diffs them — parity,
// no golden outputs.
'use strict';

let input = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (c) => { input += c; });
process.stdin.on('end', () => {
  const { derived_path, specimens, reprocess_ratios } = JSON.parse(input);
  const derived = require(derived_path);
  const out = {};
  for (const [sid, s] of Object.entries(specimens)) {
    const band = derived.reprocessBand(s);
    // Keyed by position in reprocess_ratios (float-to-string formatting
    // differs between JS and Python, so ratios don't make stable keys)
    const assessments = reprocess_ratios.map((r) => {
      const a = derived.reprocessAssessment(s, r);
      return {
        eligible: a.eligible,
        ratio: Number.isFinite(a.ratio) ? Number(a.ratio.toFixed(9)) : 'inf',
        band: a.band,
        reason: a.reason,
      };
    });
    out[sid] = { band: band.band, reason: band.reason, assessments };
  }
  process.stdout.write(JSON.stringify(out));
});
