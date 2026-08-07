// Click a diagram to open it in its own browser tab, full size.
//
// A diagram in the content column is bounded by the column. Opening it standalone hands it to
// the browser, where the whole viewport is available and the browser's own zoom, scroll, save
// and print already work — no bespoke pan/zoom code to maintain, and it behaves the way every
// other "open image in new tab" does.
//
// The tab is built from a Blob, so it needs no server and works from a file:// copy of the
// book. The click handler is DELEGATED from the document, so it does not care when mermaid
// finishes rendering: a diagram is clickable the moment it exists.
//
// The SVG mermaid emits carries its own <style>, which is why the copy in the new tab keeps
// the colours of the theme it was rendered in.

(() => {
    const openDiagram = (svg, pre) => {
        const clone = svg.cloneNode(true);

        // The page copy is sized to the column. The standalone copy should fill the tab, so
        // drop the fixed size and let the viewBox drive scaling.
        const width = svg.getAttribute('width') || svg.viewBox?.baseVal?.width || 0;
        const height = svg.getAttribute('height') || svg.viewBox?.baseVal?.height || 0;
        if (!clone.getAttribute('viewBox') && width && height) {
            clone.setAttribute('viewBox', `0 0 ${parseFloat(width)} ${parseFloat(height)}`);
        }
        clone.removeAttribute('width');
        clone.removeAttribute('height');
        clone.removeAttribute('style');
        clone.setAttribute('preserveAspectRatio', 'xMidYMid meet');
        clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');

        // Carry the reader's current theme across, so a diagram opened from the dark book
        // does not land on a white page.
        const styles = getComputedStyle(document.body);
        const bg = styles.backgroundColor || '#fff';
        const fg = styles.color || '#333';

        // A caption that says where this came from — a diagram tab with no provenance is a
        // screenshot waiting to be misattributed.
        const heading = pre.closest('main')?.querySelector('h1')?.textContent?.trim() || 'Diagram';
        const title = `${heading} — Sutra`;
        const source = location.href.split('#')[0];

        const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(title)}</title>
<style>
  html, body { margin: 0; height: 100%; background: ${bg}; color: ${fg};
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }
  body { display: flex; flex-direction: column; }
  main { flex: 1; display: flex; align-items: center; justify-content: center; padding: 1.5rem; min-height: 0; }
  svg { width: 100%; height: 100%; max-width: 100%; max-height: 100%; }
  footer { padding: 0.6rem 1rem; font-size: 0.8rem; opacity: 0.65;
    border-top: 1px solid rgba(128,128,128,0.25); }
  footer a { color: inherit; }
  @media print { footer { display: none; } main { padding: 0; } }
</style>
</head>
<body>
<main>${clone.outerHTML}</main>
<footer>${escapeHtml(heading)} · <a href="${escapeAttr(source)}">${escapeHtml(source)}</a></footer>
</body>
</html>`;

        const url = URL.createObjectURL(new Blob([html], { type: 'text/html' }));
        const tab = window.open(url, '_blank', 'noopener');
        if (!tab) {
            // Popup blocked: say so rather than appearing to do nothing.
            URL.revokeObjectURL(url);
            console.warn('sutra: the diagram tab was blocked by the browser’s popup blocker');
            return;
        }
        // The tab has its own copy of the document by now; the object URL can go.
        setTimeout(() => URL.revokeObjectURL(url), 60_000);
    };

    const escapeHtml = (s) => String(s).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
    const escapeAttr = (s) => escapeHtml(s).replace(/"/g, '&quot;');

    document.addEventListener('click', (e) => {
        const pre = e.target.closest && e.target.closest('pre.mermaid');
        if (!pre) return;
        const svg = pre.querySelector('svg');
        if (svg) openDiagram(svg, pre);
    });

    // Keyboard parity — otherwise every diagram is mouse-only.
    document.addEventListener('keydown', (e) => {
        if (e.key !== 'Enter' && e.key !== ' ') return;
        const pre = document.activeElement;
        if (!pre?.matches?.('pre.mermaid')) return;
        const svg = pre.querySelector('svg');
        if (svg) { e.preventDefault(); openDiagram(svg, pre); }
    });

    // Mark rendered diagrams focusable and announce what activating them does. Mermaid
    // renders asynchronously, so this runs again whenever the DOM changes.
    const mark = () => {
        for (const pre of document.querySelectorAll('pre.mermaid:not([data-openable])')) {
            pre.setAttribute('data-openable', '');
            pre.tabIndex = 0;
            pre.setAttribute('role', 'button');
            pre.setAttribute('aria-label', 'Diagram — activate to open it in a new tab');
        }
    };
    document.addEventListener('DOMContentLoaded', mark);
    new MutationObserver(mark).observe(document.documentElement, { childList: true, subtree: true });
})();
