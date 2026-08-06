/**
 * Workspace-config loader. Replaces the hard-coded canary registry with one derived
 * from the actual workspace state:
 *
 *   - Scans `pom.xml` files for `<dependency>` entries matching `sutra-codec-*`,
 *     `sutra-validator-*`, `sutra-trigger-*` artifact ids — those are the codec/validator/
 *     trigger ids available in this workspace.
 *   - Scans `module.yml` files for `channels:` blocks — extracts channel ids.
 *
 * Lightweight implementation (no XML library): a small regex-driven scan suffices for
 * the canary because we only need artifactId discovery, not full pom interpretation.
 */
import { promises as fs } from 'fs';
import * as path from 'path';
import type { WorkspaceRegistry } from './registry.js';

/** The well-known prefix → extension-category mapping derived from the engine's repo layout. */
const CODEC_PREFIX = 'sutra-codec-';
const VALIDATOR_PREFIX = 'sutra-validator-';
const TRIGGER_PREFIX = 'sutra-trigger-';

/**
 * Load the workspace registry by walking the workspace root for `pom.xml` and `module.yml`.
 * Returns an empty registry if the workspace root is missing or unreadable; the caller can
 * fall back to {@link defaultRegistry} in that case.
 */
export async function loadWorkspaceRegistry(workspaceRoot: string): Promise<WorkspaceRegistry> {
  const codecs = new Set<string>();
  const validators = new Set<string>();
  const channels = new Set<string>();
  const sinks = new Set<string>(['sql', 'jsonl']);

  try {
    const files = await walk(workspaceRoot);
    for (const f of files) {
      const base = path.basename(f);
      if (base === 'pom.xml') {
        await scanPom(f, codecs, validators);
      } else if (base === 'module.yml' || base === 'module.yaml') {
        await scanModuleYml(f, channels);
      }
    }
  } catch {
    // any walk failure → empty registry; caller falls back to defaults
  }

  return { codecs, validators, channels, sinks };
}

/** Recursively list files under `root`, skipping common heavy dirs. */
async function walk(root: string): Promise<string[]> {
  const skip = new Set(['node_modules', 'target', 'build', 'dist', '.git', '.idea', '.vscode']);
  const out: string[] = [];
  const queue: string[] = [root];
  while (queue.length > 0) {
    const dir = queue.shift()!;
    let entries: import('fs').Dirent[];
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      if (skip.has(e.name)) continue;
      const full = path.join(dir, e.name);
      if (e.isDirectory()) {
        queue.push(full);
      } else if (e.isFile()) {
        out.push(full);
      }
    }
  }
  return out;
}

/** Parse a pom.xml for sutra-codec-* / sutra-validator-* / sutra-trigger-* artifactIds. */
async function scanPom(file: string, codecs: Set<string>, validators: Set<string>): Promise<void> {
  let body: string;
  try {
    body = await fs.readFile(file, 'utf8');
  } catch {
    return;
  }
  const artifactRegex = /<artifactId>\s*(sutra-[a-z0-9-]+)\s*<\/artifactId>/g;
  let m: RegExpExecArray | null;
  while ((m = artifactRegex.exec(body)) !== null) {
    const id = m[1];
    if (id.startsWith(CODEC_PREFIX)) {
      codecs.add(id.substring(CODEC_PREFIX.length));
    } else if (id.startsWith(VALIDATOR_PREFIX)) {
      validators.add(id.substring(VALIDATOR_PREFIX.length));
    } else if (id.startsWith(TRIGGER_PREFIX)) {
      // Triggers double as inbound channels — they declare both source + sink for one transport.
      // The workspace's actual channel ids live in module.yml; surface the trigger transport
      // name as a discoverable channel-ish hint anyway so completions still help.
    }
  }
}

/** Parse a module.yml for `channels:` list entries. */
async function scanModuleYml(file: string, channels: Set<string>): Promise<void> {
  let body: string;
  try {
    body = await fs.readFile(file, 'utf8');
  } catch {
    return;
  }
  let inChannels = false;
  for (const raw of body.split(/\r?\n/)) {
    const line = raw.trim();
    if (line === 'channels:' || line.startsWith('channels:')) {
      inChannels = true;
      continue;
    }
    if (inChannels) {
      if (line.startsWith('- ')) {
        const ch = line.substring(2).trim().replace(/"/g, '');
        if (ch.length > 0) channels.add(ch);
      } else if (line.length > 0 && !line.startsWith('#')) {
        inChannels = false;
      }
    }
  }
}

// ───────────────────────────────────────────────────────────────────────────
// Deployment file-map gathering (the WASM lint cross-file context).
//
// The WASM lint core (`sutra-lint-core::lint`) reconstructs a deployment from
// the LOOSE editor files via the SAME `parse_deployment` the archive reader
// uses. That reader accepts ONLY the archive interior layout — `bpmn/`, `rules/`,
// `templates/`, `scripts/`, `schemas/`, `migrations/`, `coverage/`,
// `channels.yaml`, `datastores.yaml` — and REJECTS the whole request (a single
// content-invalid diagnostic) on any stray entry. So we gather strictly that
// set, keyed by the archive-local (posix) path, best-effort and bounded.
// ───────────────────────────────────────────────────────────────────────────

/** The archive-interior top-level directories the loader recognises. */
const DEPLOYMENT_TOP_DIRS = [
  'bpmn',
  'rules',
  'templates',
  'scripts',
  'schemas',
  'migrations',
  'coverage',
] as const;

/** The archive-interior top-level single files the loader recognises. */
const DEPLOYMENT_TOP_FILES = ['channels.yaml', 'datastores.yaml'] as const;

/** Bounds so a pathological workspace never wedges the editor (best-effort gather). */
const MAX_FILE_BYTES = 512 * 1024; // skip any single file larger than this
const MAX_TOTAL_FILES = 500; // cap the number of gathered entries
const MAX_TOTAL_BYTES = 12 * 1024 * 1024; // cap the cumulative gathered bytes

/** Dirs never worth descending into when gathering a deployment. */
const GATHER_SKIP_DIRS = new Set(['node_modules', 'target', 'build', 'dist', '.git', '.idea', '.vscode', 'out']);

/** The gathered deployment context handed to the WASM lint. */
export interface DeploymentFileMap {
  /** Absolute path of the resolved deployment root, or `null` when none was found. */
  root: string | null;
  /** archive-local (posix) path → UTF-8 content, e.g. `bpmn/order.bpmn` → `<xml…>`. */
  files: Record<string, string>;
}

/** The archive-local (posix) path of `absPath` relative to the deployment `root`. */
export function archivePathOf(root: string, absPath: string): string {
  return path.relative(root, absPath).split(path.sep).join('/');
}

/**
 * Locate the deployment root for an open `.bpmn` document: the archive layout is
 * `<root>/bpmn/…`, so the root is the parent of the OUTERMOST `bpmn` ancestor
 * directory. Returns `null` when the document is not under any `bpmn/` directory
 * (cross-file lint is not meaningful then — the loader keys processes by `bpmn/`).
 */
export function findDeploymentRoot(docPath: string): string | null {
  let bpmnParent: string | null = null;
  let cur = path.dirname(docPath);
  for (let guard = 0; guard < 32; guard++) {
    if (path.basename(cur) === 'bpmn') bpmnParent = path.dirname(cur);
    const parent = path.dirname(cur);
    if (parent === cur) break; // reached the filesystem root
    cur = parent;
  }
  return bpmnParent;
}

/**
 * Gather the deployment's interior files around an open `.bpmn` document into the
 * `{ archivePath: content }` map the WASM lint expects. Reads only the recognised
 * top-level dirs/files, as UTF-8, bounded by {@link MAX_FILE_BYTES} /
 * {@link MAX_TOTAL_FILES} / {@link MAX_TOTAL_BYTES}. Any unreadable/oversize entry
 * is skipped rather than failing the gather. Returns `{ root: null, files: {} }`
 * when no deployment root is found.
 */
export async function gatherDeploymentFiles(docPath: string): Promise<DeploymentFileMap> {
  const root = findDeploymentRoot(docPath);
  if (!root) return { root: null, files: {} };

  const files: Record<string, string> = {};
  const budget = { count: 0, bytes: 0 };

  for (const name of DEPLOYMENT_TOP_FILES) {
    await tryReadInto(files, root, path.join(root, name), budget);
  }
  for (const dir of DEPLOYMENT_TOP_DIRS) {
    await walkInto(files, root, path.join(root, dir), budget);
  }

  return { root, files };
}

/** Recursively read text files under `dir` into `files` (keyed archive-local), within budget. */
async function walkInto(
  files: Record<string, string>,
  root: string,
  dir: string,
  budget: { count: number; bytes: number }
): Promise<void> {
  let entries: import('fs').Dirent[];
  try {
    entries = await fs.readdir(dir, { withFileTypes: true });
  } catch {
    return; // dir absent/unreadable → nothing to add
  }
  for (const e of entries) {
    if (budget.count >= MAX_TOTAL_FILES || budget.bytes >= MAX_TOTAL_BYTES) return;
    if (GATHER_SKIP_DIRS.has(e.name)) continue;
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      await walkInto(files, root, full, budget);
    } else if (e.isFile()) {
      await tryReadInto(files, root, full, budget);
    }
  }
}

/** Read one file as UTF-8 into `files`, honouring the per-file + cumulative budget. */
async function tryReadInto(
  files: Record<string, string>,
  root: string,
  full: string,
  budget: { count: number; bytes: number }
): Promise<void> {
  if (budget.count >= MAX_TOTAL_FILES || budget.bytes >= MAX_TOTAL_BYTES) return;
  let stat: import('fs').Stats;
  try {
    stat = await fs.stat(full);
  } catch {
    return;
  }
  if (!stat.isFile() || stat.size > MAX_FILE_BYTES) return;
  if (budget.bytes + stat.size > MAX_TOTAL_BYTES) return;
  let content: string;
  try {
    content = await fs.readFile(full, 'utf8');
  } catch {
    return; // unreadable / not valid UTF-8 → skip (best-effort)
  }
  files[archivePathOf(root, full)] = content;
  budget.count += 1;
  budget.bytes += stat.size;
}
