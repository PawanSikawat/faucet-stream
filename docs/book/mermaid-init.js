// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Customized for faucet-stream: theme every diagram to the docs palette
// (warm/greige with a dark-green accent, white on green) instead of Mermaid's
// default purple, and constrain diagrams to the content width so wide flows
// scale to fit rather than overflowing and getting cut.

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

    const themeVariables = lastThemeWasLight
        ? {
              fontFamily:
                  '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
              fontSize: '14px',
              primaryColor: '#e2ece8', // node fill — soft green-tinted on warm paper
              primaryTextColor: '#1b1b18', // near-black label text
              primaryBorderColor: '#247368', // dark-green node border (the accent)
              lineColor: '#7c7c70', // warm-gray edges
              secondaryColor: '#e6e4dc',
              tertiaryColor: '#ecebe3', // edge-label / cluster background
              clusterBkg: '#e2e0d8',
              clusterBorder: '#c4c4b8',
              titleColor: '#1b1b18',
              edgeLabelBackground: '#e6e4dc',
          }
        : {
              fontFamily:
                  '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
              fontSize: '14px',
              primaryColor: '#22403a',
              primaryTextColor: '#e8e6dd',
              primaryBorderColor: '#57a894',
              lineColor: '#8f8d82',
              secondaryColor: '#2a2924',
              tertiaryColor: '#26251f',
              clusterBkg: '#26251f',
              clusterBorder: '#38372f',
              titleColor: '#e8e6dd',
              edgeLabelBackground: '#2a2924',
          };

    mermaid.initialize({
        startOnLoad: true,
        theme: 'base',
        themeVariables,
        flowchart: {
            useMaxWidth: true, // scale wide diagrams down to the content width (no overflow/cut)
            htmlLabels: true,
            curve: 'basis',
            nodeSpacing: 36,
            rankSpacing: 44,
            padding: 8,
        },
    });

    // Re-render diagrams in the correct palette when the user switches theme
    // (a reload is the simplest reliable path, matching upstream mdbook-mermaid).
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
