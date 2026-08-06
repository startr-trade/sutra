/**
 * FeelEvaluator — thin browser-side wrapper over `@bpmn-io/feelin` (the canonical
 * JS FEEL interpreter used across the bpmn-io ecosystem). The STATUS.md follow-up
 * line called this a "WASM evaluator" — pragmatically `@bpmn-io/feelin` covers the
 * same use case (live FEEL evaluation inside the in-browser modeler) without us
 * shipping a separate WASM binary; the underlying parser is a real Lezer LR
 * generator-produced grammar, so behavior matches the engine's
 * `trade.startr.sutra.runtime.feel.FeelEvaluator` for the expression subset
 * authors paste into property-panel entries (literals, arithmetic, comparison,
 * boolean, property access, list indexing, common builtins).
 *
 * Surface mirrors `FeelExpressions` on the Java side:
 *
 *   evaluate(expression, context) -> { ok, value?, error?, warnings? }
 *
 *   - `ok: true` when the expression parses and evaluates without an exception.
 *     A non-empty `warnings` array (e.g. NO_CONTEXT_ENTRY_FOUND for an undefined
 *     property) still counts as ok=true because feelin returns a defined value
 *     (typically `null`) in that case — the same contract the engine uses to keep
 *     property paths nullable.
 *   - `ok: false` when feelin throws (parse failure, runtime type error, etc.).
 *     `error` is the thrown message.
 *
 * Sample-input parse-failure handling lives in `evaluateWithSample()` so the UI
 * preview can show "Sample JSON invalid: ..." instead of crashing.
 */

import { evaluate as feelEvaluate } from '@bpmn-io/feelin';

/**
 * @param {string} expression - the FEEL expression to evaluate
 * @param {object} [context]  - context bindings (variables visible to the expression)
 * @returns {{ ok: boolean, value?: unknown, error?: string, warnings?: Array }}
 */
export function evaluate(expression, context = {}) {
  if (expression == null || expression === '') {
    return { ok: true, value: null, warnings: [] };
  }
  try {
    const result = feelEvaluate(expression, context);
    return {
      ok: true,
      value: result.value,
      warnings: result.warnings || []
    };
  } catch (e) {
    return {
      ok: false,
      error: e && e.message ? e.message : String(e)
    };
  }
}

/**
 * Convenience helper for property-panel preview entries — takes the user-typed
 * sample-input JSON string, parses it, and runs `evaluate()`. JSON parse errors
 * are returned distinctly so the UI can show a "Sample JSON invalid" message
 * separately from a FEEL evaluation error.
 *
 * @param {string} expression
 * @param {string} sampleJson - the textarea contents (may be empty / null)
 * @returns {{ ok: boolean, value?: unknown, error?: string, sampleError?: string, warnings?: Array }}
 */
export function evaluateWithSample(expression, sampleJson) {
  let context = {};
  if (sampleJson != null && String(sampleJson).trim() !== '') {
    try {
      context = JSON.parse(sampleJson);
      if (context == null || typeof context !== 'object' || Array.isArray(context)) {
        return {
          ok: false,
          sampleError: 'Sample input must be a JSON object (got ' +
            (Array.isArray(context) ? 'array' : typeof context) + ')'
        };
      }
    } catch (e) {
      return {
        ok: false,
        sampleError: 'Sample JSON invalid: ' + (e.message || String(e))
      };
    }
  }
  return evaluate(expression, context);
}

/**
 * Render the result of `evaluate()` as a short single-line string suitable for
 * showing in a property-panel preview row. Errors are prefixed; long values are
 * truncated.
 *
 * @param {ReturnType<typeof evaluate> | ReturnType<typeof evaluateWithSample>} result
 * @returns {string}
 */
export function formatResult(result) {
  if (!result) return '';
  if (result.sampleError) return 'Sample JSON invalid: ' + result.sampleError.replace(/^Sample JSON invalid: /, '');
  if (!result.ok) return 'Error: ' + result.error;
  const v = result.value;
  let rendered;
  if (v === null || v === undefined) rendered = 'null';
  else if (typeof v === 'string') rendered = JSON.stringify(v);
  else if (typeof v === 'object') {
    try { rendered = JSON.stringify(v); } catch { rendered = String(v); }
  } else rendered = String(v);
  if (rendered.length > 160) rendered = rendered.slice(0, 157) + '...';
  if (result.warnings && result.warnings.length > 0) {
    rendered += '  (' + result.warnings.length + ' warning' + (result.warnings.length === 1 ? '' : 's') + ')';
  }
  return rendered;
}

export default { evaluate, evaluateWithSample, formatResult };
