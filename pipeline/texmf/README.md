# Vendored LaTeX packages for the nightly pipeline

The cloud sessions that compile CVs and cover letters run TeX Live 2023 from Debian packages,
which lack two packages the stock moderncv CV loads. This TDS tree adds them. Sessions use it
with:

```bash
export TEXMFHOME="$PWD/pipeline/texmf"
```

The same image also lacks `texlive-luatex` (luaotfload, luatexbase, ctablestack), which can't
be vendored sensibly. `pipeline/RUNTIME.md` installs it once per session with `apt-get` before
the first `lualatex` run.

## Contents

| Package | Version | Source | Licenses |
|---|---|---|---|
| fontawesome5 | 5.15.4 (2022/05/02) | https://mirrors.ctan.org/fonts/fontawesome5.zip | TeX files: LPPL 1.3c. Fonts (Font Awesome 5 Free): SIL OFL 1.1 |
| academicons | 1.9.6-3 (2026/09/09) | https://mirrors.ctan.org/fonts/academicons.zip | TeX files: LPPL 1.3c. Font (Academicons): SIL OFL 1.1 |

Both were downloaded from CTAN on 2026-11-09 and are unmodified. Each package's own README is in
`doc/fonts/<package>/README.md`; the license texts are in `doc/licenses/`. The packages'
documentation PDFs were left out to keep the repository small; CTAN has them.

Layout (TDS): `tex/latex/<package>/` for the macro files, `fonts/{opentype,truetype,type1,tfm}/public/<package>/`
for the fonts, `fonts/{enc,map}/dvips/<package>/` for the pdfTeX encoding and map files.

Verified on 2026-11-09 in the cloud environment: with `texlive-luatex` installed and
`TEXMFHOME` pointing here, the stock moderncv CV compiles to two pages with `lualatex`, the
cover letter compiles with `xelatex`, and `tools/verify_pdf.py` and `tools/verify_layout.py`
pass on both.
