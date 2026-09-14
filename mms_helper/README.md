# NP Sample Request Helper — Chrome extension for MMS

Alex, 14-Sep-2026: MMS's own screens cannot be changed, so this sits on top
of them. Open any sample request in MMS

    http://www.npsin.com/mms3/master/sampleRequestUpdate.do?code=S-XXXXX-000

and a panel appears on the right with the standard request form:

* **pre-filled with what the SR already knows** — receiver, contact, address,
  send method, bag, budget from this SR's previous items (labelled `ITEM 3`
  so you can see where each came from); compliance proposed from the
  country in the address; need-by proposed one week out;
* **"Still missing"** — the fields R&D needs for THIS request. What is
  needed changes with the request: hand-carry drops the address and
  contact, Repeat / Modify asks for the base code;
* **"Previously on this SR"** — one-tap chips for seasonings already on
  the SR, for repeats;
* **Preview / Copy note** — the note in the house layout (Seasoning name,
  Comment, TARGET BASE, BAG, BUDGET, COMPLIANCE, QTY, NEED BY, delivery,
  receiver), the same layout the Telegram /sr bot writes;
* **Write into item N** — fills the new item's request type, base code,
  note, next-action-by and until-date in the MMS form. If the SR has no
  empty item yet it presses MMS's *Add Item* for you, then fills the item
  the page comes back with.

It **never presses Save**. You check the item and save it yourself.
Your typing is kept per SR (Chrome storage) so a page reload does not
lose the draft.

## Install (once, ~1 minute)

1. Chrome → `chrome://extensions` → switch on **Developer mode** (top right).
2. **Load unpacked** → choose this folder (`NPSampleBot/mms_helper`).
3. Open an SR in MMS. The panel is there. (Minimise it with the `–`.)

To update after a code change: `chrome://extensions` → the ↻ reload icon on
the card, then reload the MMS page.

## What it reads, and what it does not

Only the page you are on: the customer label, the saved items' notes, the
MMS dropdowns. It makes no network calls of its own and holds no
credentials — you log in to MMS as usual. Customer-master details for a
brand-new SR (no items yet) are not on the page, so those fields start
empty and show in "Still missing"; the Telegram bot's `/sr` still has that
history if you need it. A later version can ask the bot for it.

## Field names it relies on (from `sample_request.SRWriter`)

`sreq1[N].reqnote`, `sreq1[N].rtype` (new / rep / mod), `reqProductCode[N]`,
`sreq1[N].nextActUserId`, `sreq1[N].prepdateString` (`13/Sep/2026`), and the
`command` parameter (`additem`). If MMS renames any of these the panel
still shows and copies the note; only "Write into item" would need a fix.
