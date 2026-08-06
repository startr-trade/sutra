/**
 * Workspace registry — set of codec / validator / channel / sink ids that are
 * "known" in the current workspace. Used for diagnostics (unknown-id reporting)
 * and completion (autocomplete from known set).
 *
 * Two providers: {@link defaultRegistry} returns a small built-in fallback for
 * environments where no workspace is open (or the loader hasn't run yet);
 * {@link loadWorkspaceRegistry} (in workspaceConfig.ts) scans the workspace's
 * pom.xml + module.yml files to build a real registry from the workspace state.
 */

export interface WorkspaceRegistry {
  codecs: Set<string>;
  validators: Set<string>;
  channels: Set<string>;
  /** Audit sinks (bundled: sql, jsonl). */
  sinks: Set<string>;
}

const FALLBACK_IDS = ['xml', 'schema', 'dmn', 'srl'];
const FALLBACK_SINKS = ['sql', 'jsonl'];

/**
 * Built-in fallback registry. Used until {@link loadWorkspaceRegistry} returns
 * a real one — e.g. on first parse before the workspace folder is known, or
 * when the workspace has no pom.xml / module.yml yet.
 */
export function defaultRegistry(): WorkspaceRegistry {
  return {
    codecs: new Set(FALLBACK_IDS),
    validators: new Set(FALLBACK_IDS),
    channels: new Set(FALLBACK_IDS),
    sinks: new Set(FALLBACK_SINKS),
  };
}

/** Look up the set of valid ids for a given logical reference kind. */
export function knownIdsFor(registry: WorkspaceRegistry, kind: string): Set<string> {
  switch (kind) {
    case 'codec':
      return registry.codecs;
    case 'validator':
      return registry.validators;
    case 'channel':
      return registry.channels;
    case 'sink':
      return registry.sinks;
    default:
      return new Set();
  }
}
