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
5. When it's green, copy the production URL.

   The **live deployment is https://quoteweb-blue.vercel.app** — that's what
   the bot points at today.

6. Tell the bot about it by editing **`QUOTE_WEB_URL` in `config.py`**:

   ```python
   QUOTE_WEB_URL = "https://quoteweb-blue.vercel.app"
   ```

   ⚠️ **Do NOT set a `QUOTE_WEB_URL` env var on Railway — it is ignored.**
   It used to be honoured, and that caused a real outage: a stale variable
   left over from the retired `np-quote-web.vercel.app` deployment (now a
   404) silently overrode the correct value in code, so reps were handed a
   dead link with nothing in the repo to explain why. Since the URL is
   public and only changes when the app is redeployed, it now lives in
   `config.py` only — one place, no dashboard step to forget. Any leftover
   Railway variable is harmless; delete it whenever convenient.

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
