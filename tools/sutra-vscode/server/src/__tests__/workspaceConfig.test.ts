import { describe, it, expect } from 'vitest';
import { promises as fs } from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  loadWorkspaceRegistry,
  findDeploymentRoot,
  gatherDeploymentFiles,
  archivePathOf,
} from '../workspaceConfig.js';

async function withTempWorkspace(populate: (root: string) => Promise<void>): Promise<{ codecs: string[]; validators: string[]; channels: string[]; }> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'sutra-vscode-ws-'));
  try {
    await populate(root);
    const reg = await loadWorkspaceRegistry(root);
    return {
      codecs: [...reg.codecs].sort(),
      validators: [...reg.validators].sort(),
      channels: [...reg.channels].sort(),
    };
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
}

describe('loadWorkspaceRegistry', () => {
  it('extracts codec ids from a pom.xml with sutra-codec-* dependencies', async () => {
    const reg = await withTempWorkspace(async (root) => {
      await fs.writeFile(path.join(root, 'pom.xml'), `
        <project>
          <dependencies>
            <dependency>
              <groupId>trade.startr.sutra</groupId>
              <artifactId>sutra-codec-csv</artifactId>
            </dependency>
            <dependency>
              <groupId>trade.startr.sutra</groupId>
              <artifactId>sutra-codec-xml</artifactId>
            </dependency>
          </dependencies>
        </project>
      `, 'utf8');
    });
    expect(reg.codecs).toEqual(['csv', 'xml']);
    expect(reg.validators).toEqual([]);
  });

  it('extracts validator ids alongside codec ids', async () => {
    const reg = await withTempWorkspace(async (root) => {
      await fs.writeFile(path.join(root, 'pom.xml'), `
        <project>
          <dependencies>
            <dependency><artifactId>sutra-validator-dmn</artifactId></dependency>
            <dependency><artifactId>sutra-validator-schema</artifactId></dependency>
            <dependency><artifactId>sutra-codec-json</artifactId></dependency>
          </dependencies>
        </project>
      `, 'utf8');
    });
    expect(reg.codecs).toEqual(['json']);
    expect(reg.validators).toEqual(['dmn', 'schema']);
  });

  it('extracts channels from module.yml', async () => {
    const reg = await withTempWorkspace(async (root) => {
      await fs.mkdir(path.join(root, 'src', 'main', 'resources', 'bpmn', 'orders'), { recursive: true });
      await fs.writeFile(
        path.join(root, 'src', 'main', 'resources', 'bpmn', 'orders', 'module.yml'),
        `name: orders\nchannels:\n  - orders-rabbit\n  - orders-http\n`,
        'utf8'
      );
    });
    expect(reg.channels).toEqual(['orders-http', 'orders-rabbit']);
  });

  it('mixes pom.xml + multiple module.yml across the workspace', async () => {
    const reg = await withTempWorkspace(async (root) => {
      await fs.writeFile(path.join(root, 'pom.xml'),
        `<project><dependencies><dependency><artifactId>sutra-codec-csv</artifactId></dependency></dependencies></project>`,
        'utf8');
      await fs.mkdir(path.join(root, 'mod-a'), { recursive: true });
      await fs.mkdir(path.join(root, 'mod-b'), { recursive: true });
      await fs.writeFile(path.join(root, 'mod-a', 'module.yml'), `channels:\n  - a-channel\n`, 'utf8');
      await fs.writeFile(path.join(root, 'mod-b', 'module.yml'), `channels:\n  - b-channel\n  - shared\n`, 'utf8');
    });
    expect(reg.codecs).toEqual(['csv']);
    expect(reg.channels).toEqual(['a-channel', 'b-channel', 'shared']);
  });

  it('returns empty sets on a workspace with no pom.xml or module.yml', async () => {
    const reg = await withTempWorkspace(async (root) => {
      await fs.writeFile(path.join(root, 'README.md'), 'no bpm artefacts here', 'utf8');
    });
    expect(reg.codecs).toEqual([]);
    expect(reg.validators).toEqual([]);
    expect(reg.channels).toEqual([]);
  });

  it('tolerates a malformed pom.xml — extracts what it can', async () => {
    const reg = await withTempWorkspace(async (root) => {
      await fs.writeFile(path.join(root, 'pom.xml'), `
        <project>
          <dependencies>
            <dependency><artifactId>sutra-codec-json</artifactId>   <!-- end tag missing -->
            <dependency><artifactId>sutra-validator-schema</artifactId></dependency>
          </dependencies>
        </project>
      `, 'utf8');
    });
    expect(reg.codecs).toEqual(['json']);
    expect(reg.validators).toEqual(['schema']);
  });

  it('skips node_modules + target + build dirs when walking', async () => {
    const reg = await withTempWorkspace(async (root) => {
      await fs.mkdir(path.join(root, 'node_modules'), { recursive: true });
      await fs.writeFile(path.join(root, 'node_modules', 'pom.xml'),
        `<dependency><artifactId>sutra-codec-decoy</artifactId></dependency>`,
        'utf8');
      await fs.writeFile(path.join(root, 'pom.xml'),
        `<dependency><artifactId>sutra-codec-real</artifactId></dependency>`,
        'utf8');
    });
    expect(reg.codecs).toEqual(['real']);
  });
});

async function withTempDir(populate: (root: string) => Promise<void>): Promise<string> {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'sutra-vscode-dep-'));
  await populate(root);
  return root;
}

describe('findDeploymentRoot', () => {
  it('returns the parent of the bpmn/ dir for a doc under bpmn/', () => {
    const root = path.join('/w', 'app');
    expect(findDeploymentRoot(path.join(root, 'bpmn', 'order.bpmn'))).toBe(root);
    expect(findDeploymentRoot(path.join(root, 'bpmn', 'orders', 'order.bpmn'))).toBe(root);
  });

  it('returns the OUTERMOST bpmn parent when the path nests a bpmn segment', () => {
    // `/w/app/bpmn/sub/bpmn/x.bpmn` → outermost bpmn ancestor is `/w/app/bpmn` → root `/w/app`.
    const p = path.join('/w', 'app', 'bpmn', 'sub', 'bpmn', 'x.bpmn');
    expect(findDeploymentRoot(p)).toBe(path.join('/w', 'app'));
  });

  it('returns null for a doc not under any bpmn/ dir', () => {
    expect(findDeploymentRoot(path.join('/w', 'app', 'x.bpmn'))).toBeNull();
  });
});

describe('gatherDeploymentFiles', () => {
  it('gathers only the recognised interior layout, keyed archive-local (posix)', async () => {
    let docPath = '';
    const root = await withTempDir(async (r) => {
      await fs.mkdir(path.join(r, 'bpmn', 'sub'), { recursive: true });
      await fs.mkdir(path.join(r, 'schemas', 'orders'), { recursive: true });
      await fs.mkdir(path.join(r, 'rules'), { recursive: true });
      await fs.writeFile(path.join(r, 'bpmn', 'order.bpmn'), '<bpmn/>', 'utf8');
      await fs.writeFile(path.join(r, 'bpmn', 'sub', 'child.bpmn'), '<bpmn/>', 'utf8');
      await fs.writeFile(path.join(r, 'schemas', 'orders', 'x.xsd'), '<xsd/>', 'utf8');
      await fs.writeFile(path.join(r, 'rules', 'routing.dmn'), '<dmn/>', 'utf8');
      await fs.writeFile(path.join(r, 'channels.yaml'), 'channels: []', 'utf8');
      await fs.writeFile(path.join(r, 'datastores.yaml'), 'datastores: []', 'utf8');
      // NOISE the loader would reject — must be excluded by the gather.
      await fs.writeFile(path.join(r, 'README.md'), 'nope', 'utf8');
      await fs.writeFile(path.join(r, 'pom.xml'), '<project/>', 'utf8');
      await fs.mkdir(path.join(r, 'target'), { recursive: true });
      await fs.writeFile(path.join(r, 'target', 'stray.bpmn'), '<bpmn/>', 'utf8');
      docPath = path.join(r, 'bpmn', 'order.bpmn');
    });
    try {
      const { root: found, files } = await gatherDeploymentFiles(docPath);
      expect(found).toBe(root);
      expect(Object.keys(files).sort()).toEqual([
        'bpmn/order.bpmn',
        'bpmn/sub/child.bpmn',
        'channels.yaml',
        'datastores.yaml',
        'rules/routing.dmn',
        'schemas/orders/x.xsd',
      ]);
      expect(archivePathOf(root, docPath)).toBe('bpmn/order.bpmn');
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it('returns an empty map when the doc is not under a deployment root', async () => {
    const root = await withTempDir(async (r) => {
      await fs.writeFile(path.join(r, 'loose.bpmn'), '<bpmn/>', 'utf8');
    });
    try {
      const { root: found, files } = await gatherDeploymentFiles(path.join(root, 'loose.bpmn'));
      expect(found).toBeNull();
      expect(files).toEqual({});
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
