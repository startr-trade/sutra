<!-- generated-by: sutra create app (edit freely — this file is yours) -->
# deployments/

Drop packaged `.sutra` archives here — the engine watches this directory (compose mounts it
read-only at `/deployments`). Each archive is one sealed, immutable deployment: adding a file
deploys it, removing it undeploys it, and edits never happen in place (re-package instead;
the archive id is content-addressed).

```
sutra package ../../packages/<pkg>       # seal the package dir into <pkg>.sutra
cp <pkg>.sutra .                         # deploy
```
