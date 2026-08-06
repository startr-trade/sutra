/**
 * LSP server entry point for sutra BPMN files.
 *
 * Wires the connection over stdio and dispatches to the pure logic
 * modules in this directory. All "interesting" behaviour lives in
 * those modules so it can be unit-tested without standing up an LSP.
 */

import {
  createConnection,
  ProposedFeatures,
  TextDocuments,
  TextDocumentSyncKind,
  InitializeParams,
  InitializeResult,
  CompletionItem,
  CompletionItemKind,
  Diagnostic,
  DiagnosticSeverity,
  Hover,
  LocationLink,
  TextDocumentPositionParams,
} from 'vscode-languageserver/node.js';
import { TextDocument } from 'vscode-languageserver-textdocument';
import { fileURLToPath } from 'node:url';

import { parseBpmn, ParseResult } from './parser.js';
import { buildSymbolTable, SymbolTable } from './symbols.js';
import { defaultRegistry } from './registry.js';
import { computeDiagnostics } from './diagnostics.js';
import { computeCompletions } from './completion.js';
import { resolveDefinition } from './definition.js';
import { computeHover } from './hover.js';
import { buildAliasIndex, ProcessAliases } from './aliasIndex.js';
import { gatherDeploymentFiles, archivePathOf } from './workspaceConfig.js';
import { buildLintRequest, runWasmLint, mapWasmDiagnostics } from './wasmValidation.js';

const connection = createConnection(ProposedFeatures.all);
const documents = new TextDocuments(TextDocument);
const registry = defaultRegistry();

/** Debounce before the (fs + WASM) cross-file lint runs, so keystrokes don't spam it. */
const WASM_DEBOUNCE_MS = 300;

// Cache parsed state per URI to avoid re-parsing for each request, and to retain
// the intra-document TS diagnostics so the async WASM pass can re-send the two
// merged (`sendDiagnostics` replaces the whole set for a URI).
interface DocCache {
  version: number;
  symbols: SymbolTable;
  parsed: ParseResult;
  /** The intra-document (TS) diagnostics, already mapped to LSP shape. */
  tsDiags: Diagnostic[];
}
const cache = new Map<string, DocCache>();

/** Per-URI debounce timers for the cross-file WASM lint. */
const wasmTimers = new Map<string, ReturnType<typeof setTimeout>>();

/** Map an internal `BpmDiagnostic` to the LSP `Diagnostic` wire shape. */
function toLspDiagnostic(d: {
  range: Diagnostic['range'];
  severity: number;
  code: string;
  source: string;
  message: string;
}): Diagnostic {
  return {
    range: d.range,
    severity: d.severity as DiagnosticSeverity,
    code: d.code,
    source: d.source,
    message: d.message,
  };
}

connection.onInitialize((_params: InitializeParams): InitializeResult => {
  return {
    capabilities: {
      textDocumentSync: TextDocumentSyncKind.Incremental,
      completionProvider: {
        triggerCharacters: ['.', ':'],
      },
      definitionProvider: true,
      hoverProvider: true,
      diagnosticProvider: {
        interFileDependencies: false,
        workspaceDiagnostics: false,
      },
    },
  };
});

function refresh(uri: string): SymbolTable | null {
  const doc = documents.get(uri);
  if (!doc) return null;
  const existing = cache.get(uri);
  if (existing && existing.version === doc.version) return existing.symbols;

  const text = doc.getText();
  const parsed = parseBpmn(text);
  const symbols = buildSymbolTable(text, parsed);

  const tsDiags = computeDiagnostics(parsed, symbols, registry).map(toLspDiagnostic);
  cache.set(uri, { version: doc.version, symbols, parsed, tsDiags });

  // Fast path: the intra-document TS diagnostics render immediately. The heavier
  // cross-file WASM checks re-send (TS + WASM merged) once the debounce fires.
  void connection.sendDiagnostics({ uri, diagnostics: tsDiags });

  return symbols;
}

/** (Re)arm the debounced cross-file WASM lint for `uri`. */
function scheduleWasmValidation(uri: string): void {
  const prev = wasmTimers.get(uri);
  if (prev) clearTimeout(prev);
  wasmTimers.set(
    uri,
    setTimeout(() => {
      wasmTimers.delete(uri);
      void runWasmValidation(uri);
    }, WASM_DEBOUNCE_MS)
  );
}

/**
 * Gather the deployment's sibling files around the open doc, run the WASM lint,
 * and re-send the open doc's diagnostics as (intra-document TS) + (cross-file
 * WASM) merged. Best-effort: on any failure it leaves the already-sent TS
 * diagnostics untouched. Guards the document version around the async fs read so
 * a stale result never clobbers fresher diagnostics.
 */
async function runWasmValidation(uri: string): Promise<void> {
  const doc = documents.get(uri);
  const entry = cache.get(uri);
  if (!doc || !entry || entry.version !== doc.version) return;

  let docPath: string;
  try {
    docPath = fileURLToPath(uri);
  } catch {
    return; // non-file URI (untitled/virtual) → no deployment on disk
  }

  const { root, files } = await gatherDeploymentFiles(docPath);
  if (!root) return; // not inside a deployment layout → nothing cross-file to add

  // The buffer may hold unsaved edits; lint what the author currently sees.
  const archivePath = archivePathOf(root, docPath);
  files[archivePath] = doc.getText();

  const wasmDiags = runWasmLint(buildLintRequest(files));
  const mapped = mapWasmDiagnostics(wasmDiags, archivePath, entry.parsed).map(toLspDiagnostic);
  if (mapped.length === 0) return; // nothing to add beyond the TS diagnostics

  // Re-validate the version: the fs read + WASM call were async.
  const freshDoc = documents.get(uri);
  const freshEntry = cache.get(uri);
  if (!freshDoc || !freshEntry || freshEntry.version !== entry.version) return;

  void connection.sendDiagnostics({
    uri,
    diagnostics: [...freshEntry.tsDiags, ...mapped],
  });
}

documents.onDidOpen((e) => {
  refresh(e.document.uri);
  scheduleWasmValidation(e.document.uri);
});
documents.onDidChangeContent((e) => {
  refresh(e.document.uri);
  scheduleWasmValidation(e.document.uri);
});
documents.onDidClose((e) => {
  cache.delete(e.document.uri);
  const timer = wasmTimers.get(e.document.uri);
  if (timer) {
    clearTimeout(timer);
    wasmTimers.delete(e.document.uri);
  }
  void connection.sendDiagnostics({ uri: e.document.uri, diagnostics: [] });
});

connection.onCompletion((params: TextDocumentPositionParams): CompletionItem[] => {
  const doc = documents.get(params.textDocument.uri);
  if (!doc) return [];
  return computeCompletions(doc.getText(), params.position, registry).map((c) => ({
    label: c.label,
    kind: c.kind as CompletionItemKind,
    detail: c.detail,
  }));
});

connection.onDefinition((params: TextDocumentPositionParams): LocationLink[] => {
  const doc = documents.get(params.textDocument.uri);
  if (!doc) return [];
  const symbols = refresh(doc.uri);
  if (!symbols) return [];
  const links = resolveDefinition(doc.getText(), params.position, symbols);
  return links.map((l) => ({
    originSelectionRange: l.originSelectionRange,
    targetUri: doc.uri,
    targetRange: l.targetRange,
    targetSelectionRange: l.targetSelectionRange,
  }));
});

connection.onHover((params: TextDocumentPositionParams): Hover | null => {
  const doc = documents.get(params.textDocument.uri);
  if (!doc) return null;
  const symbols = refresh(doc.uri);
  if (!symbols) return null;
  const result = computeHover(doc.getText(), params.position, symbols, registry);
  if (!result) return null;
  return {
    contents: { kind: 'markdown', value: result.contents },
    range: result.range,
  };
});

/**
 * Custom request: `sutra/aliasIndex`.
 *
 * Returns the alias declarations for an open BPMN document, grouped by
 * process → start event. Backs the `AliasTreeProvider` view in the VS Code
 * extension. Returns an empty array when the document isn't open.
 */
interface AliasIndexParams {
  textDocument: { uri: string };
}
connection.onRequest(
  'sutra/aliasIndex',
  (params: AliasIndexParams): ProcessAliases[] => {
    const doc = documents.get(params.textDocument.uri);
    if (!doc) return [];
    const parsed = parseBpmn(doc.getText());
    return buildAliasIndex(doc.getText(), parsed);
  }
);

documents.listen(connection);
connection.listen();
