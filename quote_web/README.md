# NP Foods Quotation Builder — Vercel app

Single-page Next.js app that lets a sales rep build and download a NP Foods
quotation PDF in the browser. Lives in this subfolder so the bot and the web
form share one repo and one git history.

## Deploy to Vercel — first time

1. **Push the repo to GitHub** (if not already).
2. Go to <https://vercel.com/new> and pick the GitHub repo.
3. On the **Configure Project** screen, set:
   - **Framework Preset:** Next.js
   - **Root Directory:** `quote_web`  ← important, the project is in a subfolder
   - Leave Build & Output Settings on the defaults.
4. Click **Deploy**. First deploy takes ~2 minutes.
5. When it's green, copy the production URL (looks like
   `https://np-quote-web.vercel.app`).
6. Paste that URL into the bot's `.env` file:

   ```
   QUOTE_WEB_URL=https://np-quote-web.vercel.app
   ```

   On Railway: add the same env var under the bot project's Variables tab,
   then restart the bot. Once it sees the URL, `/quote` will start handing
   out clickable links.

## Run locally

```
cd quote_web
npm install
npm run dev
```

Open <http://localhost:3000>. Hot reload works for everything except the PDF
component — refresh the browser after editing `lib/pdf.tsx`.

## Files

- `app/page.tsx` — the form (single page, single component).
- `lib/pdf.tsx` — the PDF document, built with `@react-pdf/renderer`. The
  table column widths, fonts and layout match the existing Word quotation
  template.
- `lib/constants.ts` — sales people, packaging options, payment terms,
  incoterms, company address, default greeting. Edit here when something
  changes — both the form and the PDF read from this file.
- `public/np_logo.jpg` / `public/bsi.jpg` — the letterhead logos. Replace
  the files (same names) if you ever rebrand.

## How the bot deep-link works

When a rep types `/quote`, the bot sends a button that opens:

```
{QUOTE_WEB_URL}/?sales=Jay
```

The `?sales=` query param matches a name in `SALES_PEOPLE`
(`lib/constants.ts`). The form pre-selects that name in the Sales Person
dropdown so reps don't have to pick themselves every time. If the query
param is missing or doesn't match, the dropdown stays on "— pick a name —".

## Adding a new sales person

1. Edit `lib/constants.ts` → add `{ Mary: { fullName: "Mary Lim", hp: "+65 ..." } }`.
2. Also edit `quotation.py` (`SALES_PEOPLE` + `SALES_FULL_NAMES`) so the bot
   knows to pre-fill them via the URL.
3. Push. Vercel redeploys automatically.

## Adding a new packaging size / payment term / incoterm

Edit the relevant array in `lib/constants.ts`. Vercel rebuilds on push.
