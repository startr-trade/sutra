// Mermaid bootstrap for the Sutra book.
//
// Two things beyond the stock mdbook-mermaid init:
//
//  1. `useMaxWidth: false` on every diagram type. The default scales each SVG DOWN to the
//     content column, which is what made wide diagrams render at unreadable point sizes. At
//     natural size a wide diagram overflows instead — and `mermaid-fix.css` gives the block
//     its own horizontal scrollbar, so the page never scrolls sideways.
//  2. A larger base font, and `wrap: true` for sequence diagrams so long messages and notes
//     wrap inside their boxes instead of overflowing them.

(() => {
    const darkThemes = ['ayu', 'navy', 'coal'];
    const lightThemes = ['light', 'rust'];

    const classList = document.getElementsByTagName('html')[0].classList;

    let lastThemeWasLight = true;
    for (const cssClass of classList) {
        if (darkThemes.includes(cssClass)) {
            lastThemeWasLight = false;
            break;
        }
    }

    const theme = lastThemeWasLight ? 'default' : 'dark';

    mermaid.initialize({
        startOnLoad: true,
        theme,
        themeVariables: {
            fontSize: '15px',
            fontFamily: '"Open Sans", "Segoe UI", system-ui, sans-serif',
        },
        flowchart: {
            useMaxWidth: false,
            htmlLabels: true,
            padding: 10,
            nodeSpacing: 45,
            rankSpacing: 55,
        },
        sequence: {
            useMaxWidth: false,
            wrap: true,
            width: 170,
            noteMargin: 12,
            boxMargin: 12,
        },
        state: { useMaxWidth: false },
        er: { useMaxWidth: false },
    });

    // Re-rendering in the new theme is a page refresh — mermaid bakes theme colours into the
    // emitted SVG, so there is nothing to restyle in place.
    for (const darkTheme of darkThemes) {
        document.getElementById(darkTheme).addEventListener('click', () => {
            if (lastThemeWasLight) {
                window.location.reload();
            }
        });
    }

    for (const lightTheme of lightThemes) {
        document.getElementById(lightTheme).addEventListener('click', () => {
            if (!lastThemeWasLight) {
                window.location.reload();
            }
        });
    }
})();
