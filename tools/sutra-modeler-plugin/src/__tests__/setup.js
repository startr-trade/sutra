/**
 * Vitest setup — provide minimal browser shims that jsdom does not implement
 * but that bpmn-js / diagram-js touch on Modeler bootstrap.
 */

// CSS.escape is used by diagram-js's palette/escape utilities.
// jsdom (as of v24) does not expose the global `CSS` interface.
if (typeof globalThis.CSS === 'undefined' || typeof globalThis.CSS.escape !== 'function') {
  // Minimal spec-compliant escape — see https://drafts.csswg.org/cssom/#serialize-an-identifier
  function escape(value) {
    const str = String(value);
    let out = '';
    for (let i = 0; i < str.length; i++) {
      const ch = str.charCodeAt(i);
      if (ch === 0x0000) {
        out += '�';
      } else if (
        (ch >= 0x0001 && ch <= 0x001F) ||
        ch === 0x007F ||
        (i === 0 && ch >= 0x0030 && ch <= 0x0039) ||
        (i === 1 && ch >= 0x0030 && ch <= 0x0039 && str.charCodeAt(0) === 0x002D)
      ) {
        out += '\\' + ch.toString(16) + ' ';
      } else if (
        ch >= 0x0080 ||
        ch === 0x002D ||
        ch === 0x005F ||
        (ch >= 0x0030 && ch <= 0x0039) ||
        (ch >= 0x0041 && ch <= 0x005A) ||
        (ch >= 0x0061 && ch <= 0x007A)
      ) {
        out += str.charAt(i);
      } else {
        out += '\\' + str.charAt(i);
      }
    }
    return out;
  }

  globalThis.CSS = globalThis.CSS || {};
  globalThis.CSS.escape = escape;
}

// PointerEvent is referenced by diagram-js drag features on import.
if (typeof globalThis.PointerEvent === 'undefined') {
  globalThis.PointerEvent = class PointerEvent extends Event {
    constructor(type, init = {}) {
      super(type, init);
      Object.assign(this, init);
    }
  };
}

// jsdom does not implement SVGGraphicsElement.getBBox / getCTM. Provide harmless stubs
// so diagram-js can compute initial viewboxes during Modeler bootstrap. The numbers do
// not matter for property-panel registration logic.
if (typeof globalThis.SVGElement !== 'undefined') {
  const noBBox = { x: 0, y: 0, width: 100, height: 100 };
  if (!globalThis.SVGElement.prototype.getBBox) {
    globalThis.SVGElement.prototype.getBBox = function () { return noBBox; };
  }
  if (!globalThis.SVGElement.prototype.getCTM) {
    globalThis.SVGElement.prototype.getCTM = function () {
      return { a: 1, b: 0, c: 0, d: 1, e: 0, f: 0 };
    };
  }
  if (!globalThis.SVGElement.prototype.getScreenCTM) {
    globalThis.SVGElement.prototype.getScreenCTM = function () {
      return { a: 1, b: 0, c: 0, d: 1, e: 0, f: 0, inverse() { return this; } };
    };
  }
}
