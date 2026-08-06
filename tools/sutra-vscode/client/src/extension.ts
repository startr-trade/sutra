/**
 * VS Code extension entry point.
 *
 * Spawns the LSP server as a Node child process over IPC and registers
 * the client for the `sutra` language. Also wires up the two read-side
 * surfaces:
 *
 *   - `sutra.openBpmnPreview` command + `sutra.bpmnPreview` custom editor
 *     (read-only `bpmn-js` rendering of the active .bpmn file).
 *   - `sutra-explorer` view container hosting the alias-index tree.
 *
 * Both surfaces are backed by the LSP — the preview just mirrors document
 * text, and the alias tree fans out the server-side `sutra/aliasIndex`
 * custom request.
 */

import * as path from 'node:path';
import * as vscode from 'vscode';
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from 'vscode-languageclient/node.js';

import { BpmnPreviewProvider } from './preview/BpmnPreviewProvider.js';
import { AliasTreeProvider } from './aliasBrowser/AliasTreeProvider.js';

let client: LanguageClient | undefined;

export function activate(context: vscode.ExtensionContext): void {
  const serverModule = context.asAbsolutePath(path.join('server', 'out', 'server.js'));

  const serverOptions: ServerOptions = {
    run: { module: serverModule, transport: TransportKind.ipc },
    debug: {
      module: serverModule,
      transport: TransportKind.ipc,
      options: { execArgv: ['--nolazy', '--inspect=6009'] },
    },
  };

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: 'file', language: 'sutra' }],
    synchronize: {},
  };

  client = new LanguageClient('sutra', 'sutra LSP', serverOptions, clientOptions);
  void client.start();

  // ── BPMN preview ────────────────────────────────────────────────────
  const previewProvider = new BpmnPreviewProvider(context);
  context.subscriptions.push(
    vscode.window.registerCustomEditorProvider(BpmnPreviewProvider.viewType, previewProvider, {
      webviewOptions: { retainContextWhenHidden: true },
      supportsMultipleEditorsPerDocument: true,
    }),
    vscode.window.registerWebviewPanelSerializer(BpmnPreviewProvider.viewType, previewProvider),
    vscode.commands.registerCommand('sutra.openBpmnPreview', (uri?: vscode.Uri) =>
      BpmnPreviewProvider.openPreview(uri)
    )
  );

  // ── Alias-index browser ─────────────────────────────────────────────
  const aliasTree = new AliasTreeProvider(() => client);
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider('sutra.aliasIndex', aliasTree),
    vscode.commands.registerCommand('sutra.refreshAliasIndex', () => aliasTree.refresh()),
    vscode.workspace.onDidSaveTextDocument((doc) => {
      if (doc.languageId === 'sutra') aliasTree.refresh();
    }),
    vscode.workspace.onDidChangeTextDocument((e) => {
      if (e.document.languageId === 'sutra') aliasTree.refresh();
    }),
    vscode.window.onDidChangeActiveTextEditor(() => aliasTree.refresh())
  );
}

export function deactivate(): Thenable<void> | undefined {
  return client?.stop();
}
