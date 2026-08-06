/**
 * feelPreview — shared builder for the "Sample input (JSON)" + "Preview result"
 * pair of property-panel entries that sit immediately below any FEEL expression
 * entry (q:alias `expression`, q:dispatch `key`, future q:case `when`).
 *
 * Why this helper exists:
 *   - The bpmn-js-properties-panel@5 entry contract is `{ id, component, label,
 *     description, get, set, element }`. Components are Inferno function refs
 *     that hosts render — under unit tests we cannot bind a real Inferno tree,
 *     so the existing groups use *Stub function components and assert at the
 *     entry-descriptor level. We mirror that posture here: shipping
 *     `TextareaEntryStub` + `ReadOnlyEntryStub` so the same descriptor shape
 *     round-trips cleanly through vitest while still letting a real bpmn-js
 *     host wire it into the live preview at runtime via the `get()` callbacks.
 *   - Sample-input is *session-local* (per-author, not persisted into the BPMN
 *     XML — it's preview-only). We hold it in a closure-scoped Map keyed by the
 *     element id so re-renders inside the panel don't lose the user's typed
 *     payload, but it never escapes into the saved diagram.
 *
 * The helper exposes two entry descriptors so they can be appended (in order)
 * into any group's `entries` array:
 *
 *   sampleEntry(element, baseId)  → textarea, persists into the in-memory map
 *   previewEntry(element, baseId, getExpression)
 *                                  → read-only preview line, evaluates live
 *                                    `expression` against the parsed sample
 */

import { evaluateWithSample, formatResult } from '../../feel/FeelEvaluator.js';

const sampleByElement = new Map();

/** Test-only — clears the per-element sample buffer so unit tests stay isolated. */
export function __resetSampleCache() {
  sampleByElement.clear();
}

function elementKey(element) {
  const bo = element && (element.businessObject || element);
  return (bo && bo.id) || '<anon>';
}

function getSample(element) {
  const k = elementKey(element);
  return sampleByElement.has(k) ? sampleByElement.get(k) : '';
}

function setSample(element, value) {
  const k = elementKey(element);
  sampleByElement.set(k, value == null ? '' : String(value));
}

function TextareaEntryStub() { return null; }
function ReadOnlyEntryStub() { return null; }

/**
 * Build the "Sample input (JSON)" textarea entry.
 *
 * @param {object} element  - bpmn-js shape / business object wrapper
 * @param {string} baseId   - e.g. 'q-alias' or 'q-dispatch'; entry id becomes `${baseId}-sample`
 * @returns {object} entry descriptor
 */
export function sampleEntry(element, baseId) {
  return {
    id: baseId + '-sample',
    component: TextareaEntryStub,
    label: 'Sample input (JSON)',
    description: 'Author-only payload (not saved). The FEEL expression above is evaluated against this object on every keystroke.',
    rows: 4,
    get: () => ({ value: getSample(element) }),
    set: (state, value) => {
      setSample(element, value);
      return { value: getSample(element) };
    },
    element,
    __preview: true
  };
}

/**
 * Build the read-only "Preview result" entry. The provided getExpression()
 * callback is invoked at render time so the preview stays live as the user
 * edits the underlying FEEL expression in the entry above it.
 *
 * @param {object} element              - bpmn-js shape / business object wrapper
 * @param {string} baseId               - e.g. 'q-alias' or 'q-dispatch'
 * @param {() => string} getExpression  - returns the current expression string
 * @returns {object} entry descriptor
 */
export function previewEntry(element, baseId, getExpression) {
  return {
    id: baseId + '-preview',
    component: ReadOnlyEntryStub,
    label: 'Preview result',
    description: 'Live result of evaluating the FEEL expression against the sample input.',
    get: () => {
      const expr = getExpression() || '';
      const sample = getSample(element);
      const result = evaluateWithSample(expr, sample);
      return {
        value: formatResult(result),
        result
      };
    },
    element,
    __preview: true,
    readOnly: true
  };
}

export const __testing = { getSample, setSample, elementKey };
