/* tslint:disable */
/* eslint-disable */

/**
 * The wasm-bindgen boundary the VS Code LSP calls as `lint(requestJson) -> diagnosticsJson`.
 * Delegates to [`lint_to_json`]; only compiled for the wasm target so the native build is
 * unchanged.
 */
export function lint(request_json: string): string;
