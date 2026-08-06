/**
 * BPMN preview pane.
 *
 * Read-only `vscode.CustomTextEditorProvider` that renders a `.bpmn` file in
 * a webview via `bpmn-js`. Users opt in (the text editor remains default) by
 * either invoking the `sutra.openBpmnPreview` command or by selecting
 * "Open With… → sutra BPMN preview" from the editor selector.
 *
 * The provider keeps the webview in sync with the underlying text document
 * — every change triggers `importXML` again so the diagram reflects the
 * current source. Parse errors surface as a VS Code error notification so
 * authors don't sit looking at a stale diagram wondering why.
 */

import * as vscode from 'vscode';

const HTML_PATH = ['client', 'media', 'preview.html'];
const BPMN_JS_PATH = ['node_modules', 'bpmn-js', 'dist', 'bpmn-viewer.production.min.js'];
const VIEW_TYPE = 'sutra.bpmnPreview';

export class BpmnPreviewProvider
  implements vscode.CustomTextEditorProvider, vscode.WebviewPanelSerializer {
  static readonly viewType = VIEW_TYPE;

  constructor(private readonly context: vscode.ExtensionContext) {}

  // ──────────────────────────────────────────────────────────────────────
  // CustomTextEditorProvider
  // ──────────────────────────────────────────────────────────────────────

  async resolveCustomTextEditor(
    document: vscode.TextDocument,
    webviewPanel: vscode.WebviewPanel,
    _token: vscode.CancellationToken
  ): Promise<void> {
    webviewPanel.webview.options = {
      enableScripts: true,
      localResourceRoots: [this.context.extensionUri],
    };
    webviewPanel.webview.html = await this.renderHtml(webviewPanel.webview);

    const post = (text: string): void => {
      void webviewPanel.webview.postMessage({ type: 'bpmn:source', text });
    };

    // Initial push happens once the webview signals it's ready.
    const messageSub = webviewPanel.webview.onDidReceiveMessage((msg) => {
      if (!msg || typeof msg !== 'object') return;
      switch (msg.type) {
        case 'webview:ready':
          post(document.getText());
          break;
        case 'bpmn:import-error':
          void vscode.window.showErrorMessage(
            `BPMN preview failed to render: ${String(msg.message ?? 'unknown error')}`
          );
          break;
      }
    });

    const changeSub = vscode.workspace.onDidChangeTextDocument((e) => {
      if (e.document.uri.toString() === document.uri.toString()) {
        post(e.document.getText());
      }
    });

    webviewPanel.onDidDispose(() => {
      messageSub.dispose();
      changeSub.dispose();
    });
  }

  // ──────────────────────────────────────────────────────────────────────
  // WebviewPanelSerializer
  // ──────────────────────────────────────────────────────────────────────

  async deserializeWebviewPanel(
    webviewPanel: vscode.WebviewPanel,
    state: { uri?: string } | undefined
  ): Promise<void> {
    webviewPanel.webview.options = {
      enableScripts: true,
      localResourceRoots: [this.context.extensionUri],
    };
    webviewPanel.webview.html = await this.renderHtml(webviewPanel.webview);

    if (state?.uri) {
      try {
        const uri = vscode.Uri.parse(state.uri);
        const doc = await vscode.workspace.openTextDocument(uri);
        const post = (text: string): void => {
          void webviewPanel.webview.postMessage({ type: 'bpmn:source', text });
        };
        webviewPanel.webview.onDidReceiveMessage((msg) => {
          if (msg?.type === 'webview:ready') post(doc.getText());
        });
      } catch {
        // Document is gone — leave the panel empty.
      }
    }
  }

  // ──────────────────────────────────────────────────────────────────────
  // Public command target — used by `sutra.openBpmnPreview`.
  // ──────────────────────────────────────────────────────────────────────

  static openPreview(uri: vscode.Uri | undefined): Thenable<unknown> {
    const target =
      uri ??
      (vscode.window.activeTextEditor &&
      vscode.window.activeTextEditor.document.languageId === 'sutra'
        ? vscode.window.activeTextEditor.document.uri
        : undefined);
    if (!target) {
      void vscode.window.showErrorMessage(
        'Open a .bpmn file before running bpm: Open BPMN preview'
      );
      return Promise.resolve(undefined);
    }
    return vscode.commands.executeCommand(
      'vscode.openWith',
      target,
      VIEW_TYPE,
      vscode.ViewColumn.Beside
    );
  }

  // ──────────────────────────────────────────────────────────────────────
  // Internals
  // ──────────────────────────────────────────────────────────────────────

  private async renderHtml(webview: vscode.Webview): Promise<string> {
    const htmlOnDisk = vscode.Uri.joinPath(this.context.extensionUri, ...HTML_PATH);
    const bpmnViewerOnDisk = vscode.Uri.joinPath(this.context.extensionUri, ...BPMN_JS_PATH);
    const bpmnViewerUri = webview.asWebviewUri(bpmnViewerOnDisk);

    let template: string;
    try {
      const bytes = await vscode.workspace.fs.readFile(htmlOnDisk);
      template = new TextDecoder().decode(bytes);
    } catch (err) {
      return `<!doctype html><html><body><p>Failed to load preview template: ${String(err)}</p></body></html>`;
    }

    return template
      .replace(/\$\{BPMN_VIEWER_URI\}/g, bpmnViewerUri.toString())
      .replace(/\$\{CSP_SOURCE\}/g, webview.cspSource);
  }
}
