/**
 * Alias-index tree view.
 *
 * Renders the alias declarations of the active BPMN editor as a 3-level
 * tree: Process → StartEvent → Alias entry. Backed by the LSP `sutra/aliasIndex`
 * custom request — the server walks the live document text and returns a
 * stable shape so the view doesn't need to re-parse client-side.
 *
 * The tree refreshes when the active editor changes and when any BPMN file
 * is saved (the request always re-asks the server, so unsaved edits are
 * surfaced too via `onDidChangeTextDocument`).
 */

import * as vscode from 'vscode';
import type { LanguageClient } from 'vscode-languageclient/node.js';

/** Server-side payload — must stay in sync with `server/src/aliasIndex.ts`. */
export interface AliasEntry {
  label: string;
  expression: string;
  onConflict: string;
  multiValue: boolean;
  range: {
    start: { line: number; character: number };
    end: { line: number; character: number };
  };
}

export interface StartEventAliases {
  startEventId: string;
  startEventName: string;
  aliases: AliasEntry[];
}

export interface ProcessAliases {
  processId: string;
  processName: string;
  startEvents: StartEventAliases[];
}

type AliasNode =
  | { kind: 'empty'; message: string }
  | { kind: 'process'; uri: vscode.Uri; process: ProcessAliases }
  | { kind: 'startEvent'; uri: vscode.Uri; processId: string; startEvent: StartEventAliases }
  | {
      kind: 'alias';
      uri: vscode.Uri;
      processId: string;
      startEventId: string;
      alias: AliasEntry;
    };

export class AliasTreeProvider implements vscode.TreeDataProvider<AliasNode> {
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<AliasNode | undefined>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  constructor(private readonly client: () => LanguageClient | undefined) {}

  refresh(): void {
    this._onDidChangeTreeData.fire(undefined);
  }

  getTreeItem(node: AliasNode): vscode.TreeItem {
    switch (node.kind) {
      case 'empty': {
        const it = new vscode.TreeItem(node.message, vscode.TreeItemCollapsibleState.None);
        it.iconPath = new vscode.ThemeIcon('info');
        return it;
      }
      case 'process': {
        const label = node.process.processName || node.process.processId || '(unnamed process)';
        const it = new vscode.TreeItem(label, vscode.TreeItemCollapsibleState.Expanded);
        it.iconPath = new vscode.ThemeIcon('symbol-namespace');
        it.description = node.process.processId;
        it.contextValue = 'sutra.process';
        return it;
      }
      case 'startEvent': {
        const label =
          node.startEvent.startEventName ||
          node.startEvent.startEventId ||
          '(unnamed start event)';
        const collapsible =
          node.startEvent.aliases.length > 0
            ? vscode.TreeItemCollapsibleState.Expanded
            : vscode.TreeItemCollapsibleState.None;
        const it = new vscode.TreeItem(label, collapsible);
        it.iconPath = new vscode.ThemeIcon('symbol-event');
        it.description = `${node.startEvent.aliases.length} alias${
          node.startEvent.aliases.length === 1 ? '' : 'es'
        }`;
        it.contextValue = 'sutra.startEvent';
        return it;
      }
      case 'alias': {
        const a = node.alias;
        const label = a.label || a.expression || '(empty alias)';
        const it = new vscode.TreeItem(label, vscode.TreeItemCollapsibleState.None);
        it.iconPath = new vscode.ThemeIcon('symbol-key');
        const flags = [a.onConflict];
        if (a.multiValue) flags.push('multi');
        it.description = `${a.expression} · ${flags.join(' · ')}`;
        it.tooltip = new vscode.MarkdownString(
          `**Expression:** \`${a.expression}\`\n\n` +
            `**On conflict:** ${a.onConflict}\n\n` +
            `**Multi-value:** ${a.multiValue ? 'yes' : 'no'}`
        );
        it.contextValue = 'sutra.alias';
        it.command = {
          command: 'vscode.open',
          title: 'Reveal alias',
          arguments: [
            node.uri,
            {
              selection: new vscode.Range(
                new vscode.Position(a.range.start.line, a.range.start.character),
                new vscode.Position(a.range.end.line, a.range.end.character)
              ),
            },
          ],
        };
        return it;
      }
    }
  }

  async getChildren(node?: AliasNode): Promise<AliasNode[]> {
    const editor = vscode.window.activeTextEditor;
    if (!editor || editor.document.languageId !== 'sutra') {
      return node ? [] : [{ kind: 'empty', message: 'Open a .bpmn file to see aliases' }];
    }

    if (!node) {
      const client = this.client();
      if (!client) {
        return [{ kind: 'empty', message: 'Language server is not running' }];
      }
      let processes: ProcessAliases[] = [];
      try {
        processes = await client.sendRequest<ProcessAliases[]>('sutra/aliasIndex', {
          textDocument: { uri: editor.document.uri.toString() },
        });
      } catch (err) {
        return [{ kind: 'empty', message: `Alias request failed: ${String(err)}` }];
      }
      if (!processes || processes.length === 0) {
        return [{ kind: 'empty', message: 'No processes in this document' }];
      }
      return processes.map((p) => ({ kind: 'process', uri: editor.document.uri, process: p }));
    }

    if (node.kind === 'process') {
      if (node.process.startEvents.length === 0) {
        return [{ kind: 'empty', message: 'No start events with aliases' }];
      }
      return node.process.startEvents.map((s) => ({
        kind: 'startEvent',
        uri: node.uri,
        processId: node.process.processId,
        startEvent: s,
      }));
    }

    if (node.kind === 'startEvent') {
      return node.startEvent.aliases.map((a) => ({
        kind: 'alias',
        uri: node.uri,
        processId: node.processId,
        startEventId: node.startEvent.startEventId,
        alias: a,
      }));
    }

    return [];
  }
}
