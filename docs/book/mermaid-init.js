// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Themes every mermaid diagram in the docs to the faucet-stream brand greens —
// dark green (#1e5448, the "Architect reference" button fill) for text/borders/
// lines, light green for node fills — so diagrams match the site instead of
// mermaid's default purple. Light/dark variants track the mdBook theme.
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

    const fontFamily = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';

    const lightVars = {
        primaryColor: '#d7e5df',        // node fill — light green (callout)
        primaryTextColor: '#14453b',    // node text — dark green
        primaryBorderColor: '#1e5448',  // node border — button dark green
        lineColor: '#1e5448',           // edges + arrows
        secondaryColor: '#eef4f1',
        secondaryTextColor: '#14453b',
        secondaryBorderColor: '#1e5448',
        tertiaryColor: '#f3f8f5',
        tertiaryTextColor: '#14453b',
        tertiaryBorderColor: '#3f9d7a',
        edgeLabelBackground: '#d7e5df',
        clusterBkg: '#eef4f1',
        clusterBorder: '#1e5448',
        titleColor: '#1e5448',
        fontFamily,
    };

    const darkVars = {
        primaryColor: '#1e5448',        // node fill — dark green (button)
        primaryTextColor: '#e8f3ee',    // node text — near white
        primaryBorderColor: '#3f9d7a',
        lineColor: '#6fbf9e',
        secondaryColor: '#163f36',
        secondaryTextColor: '#e8f3ee',
        secondaryBorderColor: '#3f9d7a',
        tertiaryColor: '#12312a',
        tertiaryTextColor: '#e8f3ee',
        tertiaryBorderColor: '#3f9d7a',
        edgeLabelBackground: '#163f36',
        clusterBkg: '#12312a',
        clusterBorder: '#3f9d7a',
        titleColor: '#6fbf9e',
        fontFamily,
    };

    mermaid.initialize({
        startOnLoad: true,
        theme: 'base',
        themeVariables: lastThemeWasLight ? lightVars : darkVars,
        flowchart: { htmlLabels: true, padding: 12 },
    });

    // Re-render diagrams in the new palette when the reader switches theme.
    for (const darkTheme of darkThemes) {
        const el = document.getElementById(darkTheme);
        if (el) {
            el.addEventListener('click', () => {
                if (lastThemeWasLight) {
                    window.location.reload();
                }
            });
        }
    }

    for (const lightTheme of lightThemes) {
        const el = document.getElementById(lightTheme);
        if (el) {
            el.addEventListener('click', () => {
                if (!lastThemeWasLight) {
                    window.location.reload();
                }
            });
        }
    }
})();
