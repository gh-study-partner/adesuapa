# Study Partner

A static front-end (`index.html`) plus small Vercel serverless functions in `api/`.
No build step — deploy as-is.

## 0. What is in this folder (5 files, no folders)

```
server.js      the whole server: accounts, ads, photos, YouTube, news AND the search engine
index.html     the whole app (the search screen and the day pictures are built into it)
setup.html     open /setup after deploying — it checks everything for you
vercel.json    tells Vercel exactly what to build (everything else in the repo is ignored)
package.json
```

**Uploading to GitHub (no folders to get wrong):** open your repo → **Add file → Upload files** → drag these 5 files in →
**Commit changes**. Files with the same name are replaced. Leftover files from earlier attempts (`index.js`, `_engine.js`,
`cron.js`, `search.js`, `*.svg`, `mon.jpg`…) do no harm — `vercel.json` ignores them — you can delete them later.

## 1. Deploy to Vercel

**Dashboard (easiest):** go to https://vercel.com/new, choose "Deploy without Git" /
drag-and-drop, and drop this whole folder in.

**CLI:** `npm i -g vercel`, then from inside this folder run `vercel`, then `vercel --prod`.

**GitHub:** push this folder to a repo and import it at https://vercel.com/new.
Framework preset "Other" — no build command, output directory is `/`.

## 2. Shared accounts, ads and home pictures (Upstash Redis)

Without a database every phone keeps its own private copy of everything. With it connected, accounts,
profile pictures, activities, ads and home-page photos follow every student on every device.

> **Fastest way to check:** after deploying, open `https://YOUR-SITE.vercel.app/setup`. It says ✅ or ❌ in plain words and
> shows the exact steps. If it is red, accounts/ads/photos stay on each phone — that is the only reason they would not be shared.
> Any name Vercel gives the database settings is detected automatically (even a custom prefix). **Always redeploy after connecting.**
>
> **Already have accounts, ads or photos made before connecting?** Students just log in once — their account moves online by
> itself. As admin, open the dashboard on the phone where you made the ads/photos and tap the green **Move them to the shared
> database** button.

1. Vercel → your project → **Storage** → **Create Database** → **Upstash for Redis** (free plan).
2. Connect it to the project, then **Deployments → ⋯ → Redeploy**.
3. Check: open `https://YOUR-SITE.vercel.app/api?a=health` → it must say `"ready":true`.

What this gives you:
- **Login from any device** brings back the student's name, school/level, profile picture, display
  preference (white/night) and their Activity timetable.
- **Ads and the seven home-page photos** posted by the admin show on every phone and account (this is the one thing that
  *needs* the database — without it each phone only sees its own). Monday–Sunday also have built-in pictures, so every day has a photo from the start. Pictures are stored
  once and served from their own cached links, so the home page stays small and fast on mobile data.
- A crowd on one Wi-Fi network is fine: sign-ups/log-ins are limited per account name, not per network,
  so one shared campus IP can't lock out a classroom (150 sign-ups from one IP were tested).

**It keeps working on slow connections.** The app now retries the server with longer waits (a freshly woken
server can take several seconds on mobile data), remembers the last good ads and day photos on each device so
the Home slideshow is instant, and refreshes them every minute and whenever the app is reopened. If a device
really cannot reach the server it says so — it never silently creates a this-phone-only account.

**Check it in one tap.** Sign in as admin → **Status** tab → **Run check**. It confirms the database, loads every
day photo and every live ad, and shows which optional keys are set, with a plain-English fix for anything wrong.

**Home slideshow.** Slide 1 is today's photo, slide 2 is the student's plan for today (sorted by time; if today is
empty it shows the next day that has activities), slide 3 is the live ad. The Activity page now opens on today.

Still saved only on the device that created it: Library courses (files, notes, links), PASCO items,
course progress/certificates and study-timer history.

**Important — change the admin login before launch.** The default (`Yo Lord!` / `#124#`) is visible in
the public code. Sign in as admin → **Admin login** tab → set your own name and password. The dashboard
shows a red warning until you do.

## 3. Search — Study Partner's own search engine (no API keys)

**Search** is in the main menu, and the floating 🔍 button on Library, PASCO and Online Courses opens it too.
Nothing here needs Gemini or any paid key. It works like a small Google:

1. **Ask many sources at once** — Bing, DuckDuckGo, Mojeek, Marginalia and Wikipedia (plus Stack Overflow for coding questions).
2. **Fuse and re-rank** the results (reciprocal-rank fusion, plus a bonus for trusted sites like .edu/.gov/Wikipedia/BBC and a
   penalty for shopping/social pages).
3. **Read the best pages** on the server, strip adverts and menus, and pick the sentences that actually answer the question.
4. **Answer with sources** — every sentence has a numbered source you can tap to open *inside the app*.

What students get:
- **Instant answers** (no search needed): calculator (`15% of 200`, `sqrt(144)`), unit conversion (`5 km to miles`),
  currency (`100 usd to ghs`), weather (`weather in Accra`), local time (`time in London`), dictionary (`define ephemeral`).
- **Question-aware answers:** "differentiate between A and B" → comparison table; "types of / steps to…" → numbered list
  found on the web; definitions → a clear lead plus key points; **Go deeper** → a study note built from the whole Wikipedia article.
- **Read any result inside the app.** Tapping a result opens a clean, readable version in the page (most websites refuse to
  be shown in someone else's frame, so the server fetches the text instead). If a site blocks that, an "Open original ↗" link is shown.
- **Videos** play inside the page (YouTube) with an "Open in YouTube app ↗" link; **News** from Google News.
- **Suggestions** while typing, **recent searches**, and a **Search on Google ↗** link under every result for hard questions.

**It learns every day.** The first search or visit to the Search page each day starts the daily learning run in the background
(no scheduler to set up), and admin → Search → **Learn now** runs it on demand. Each run reads
today's trending Wikipedia topics, Ghana and world headlines and six rotating syllabus topics (about 130 school/university topics in
`server.js`, edit the `CURRICULUM` list to suit your courses), adds them to a knowledge base in Redis (newest 900 pages), and
refreshes the answers students searched for most. Every student search also adds what it read to the knowledge base, and
the Search home page shows "Trending today", "Study topics learned today" and "Popular with students". Searches that
only one person made are never shown to others.

**Check it in one tap:** sign in as admin → **Search** tab → **Test the engine**. It tests every source *from your Vercel
server* and shows which ones respond. **Learn now** runs the daily job immediately.

> **Honest note on reliability.** The engine uses public pages that no one promised to keep stable (Bing/DuckDuckGo/Mojeek
> result pages). If one changes or blocks Vercel's servers, the engine keeps going on the others — Wikipedia, Marginalia, the
> knowledge base and the instant answers don't depend on them. The Test button tells you exactly what is working today.
> The code was tested here against recorded copies of those pages, not against the live internet, so run the Test button after deploying.

Optional: a free `YOUTUBE_API_KEY` (Google Cloud → YouTube Data API v3) makes video search more dependable; `CRON_SECRET`
(any random text) locks the daily job so only Vercel can run it.

### Research while you study (nothing closes)

Open any slide, PDF, Word/Excel file or text note and tap the purple **🔍 Research** button in its header (or the round 🔍
button on Library, PASCO, Online Courses and Timetable). A **small Research window** appears over the page — the file stays open
behind it and keeps its place. On a computer, drag the window by its title bar. Buttons in its header:
**⇔** put it beside the file instead (❐ returns to the small window), **⇕** bigger / smaller, **—** minimise, **✕** close.
On a phone it is a bottom sheet. Select any word or sentence in a document, note or page and tap the "🔍 Search" button that pops up.
If the full search engine can't be reached, it still answers from Wikipedia.

## 4. Slides and documents

Everything opens inside Study Partner; nothing has to be downloaded to be read.

| File | What students see |
|---|---|
| **PDF** | Every page, in-app |
| **PowerPoint .pptx** | **Converted to pictures in the background right after upload** (the card shows "Preparing slides… 3/24", then "✓ Slides ready"), then opened like PDF pages — instant, and identical every time. Slides are drawn by a **built-in renderer** (theme colours and fonts, backgrounds, titles and bullets, numbering, pictures — including photos dropped into layout placeholders, pictures inside PowerPoint's compatibility wrappers, and picture backgrounds from the theme — with crop, charts (column, bar, line, area, pie), SmartArt, tables, shapes, lines, groups) that needs no download from the internet. A 60-slide lecture converts in about 7 seconds. Files uploaded before this update are converted the first time they are opened |
| **Old .ppt (1997–2003)** | Best-effort in-app viewer from a third party (small "Flyfish Viewer" mark). If it cannot load, the **text of every slide** opens in the page instead. **Best practice: Save As .pptx before uploading** |
| **Word .docx** | The document text, in-app |
| **Old .doc** | The document text opens in the page (headings, tables, accented and Ghanaian letters; no pictures). Save As .docx or PDF for the full look |
| **Links** | Google Drive / Slides / Docs and YouTube links embed in-app and work for any file type, including old .ppt (Drive: set sharing to "Anyone with the link") |
| **Excel .xlsx, CSV, text, RTF** | Shown as tables or clean text, with a tab for each Excel sheet |
| **Pictures** (PNG, JPG, WebP, GIF) | Shown in the page |
| **Anything else** | An explanation in the page. **Nothing is ever downloaded automatically** — a "Download original" button is there only if the student chooses it |

**Picture conversion, in detail.** It runs on the student's own phone or computer while they keep using the app;
nothing is sent to any server. The pictures are saved beside the file and are removed when the course is
deleted. Before saving, the app checks that each picture really contains the slide — if a browser can't
make pictures (or draws a blank one), nothing is saved and the slides are simply drawn live instead.
Because the pictures live on that device, a student who opens the same course on another phone gets them
built again on first open.

How the .pptx viewer decides: saved pictures → live built-in renderer → a second viewer → a text view of every
slide. The **Switch viewer** button cycles through them if a slide ever looks wrong.
**Not drawn:** animations, video/audio, old EMF/WMF clip-art pictures and some special effects. Those show as a
labelled box. For slides that rely on them, upload a PDF export, which opens exactly as designed.

Files up to **40 MB** are accepted. They are stored in the browser (IndexedDB) of the device that uploaded
them and survive reloads.

## 5. PASCO and the Library work the same way

PASCO (past questions) accepts exactly what the Library accepts — PDF, PowerPoint, Word, Excel, pictures, text
files and Google Drive/Slides/Docs/YouTube links — and opens them with the same in-page viewers, so nothing needs
to be downloaded. PowerPoint files are turned into slide pictures in the background and show "✓ Slides ready", and
every PASCO item has a ✕ button that removes it together with its stored file and pictures. Pasting questions still
turns them into an interactive quiz, and can be combined with a file or link in one go.

### PASCO fixes

Uploading a file *and* pasting questions at the same time used to silently drop the
pasted questions — both are now saved as separate items. The question parser is also
far more forgiving now: the "Q:" prefix is optional, and "Answer:", "Ans:", or
"Correct:" are all recognized (previously only an exact "Answer:" line worked).

## 6. Admin login

There's no visible "Admin" link anywhere in the app — sign in on the normal sign-up form
using the special name/password (defaults: `Yo Lord!` / `#124#`).

You can change the admin name/password two ways:
- **From inside the app** (once shared accounts/database are connected — see step 2):
  sign in as admin → **Admin login** tab in the dashboard → enter a new name and/or
  password plus your current password to confirm → Save. This is the easiest way and
  doesn't need Vercel at all.
- **Via Vercel**, by setting `ADMIN_USER` and `ADMIN_PASS` as environment variables —
  this only sets the *starting* login before you've changed it from inside the app once;
  after your first in-app change, the in-app one takes over.

## 7. Ads: timing, and running your own

Every paid ad expires automatically based on the plan the business picked — 24 hours,
48 hours, 3 days, 1 week, or 1 month — counted from the moment **you approve it**, not
from when they paid. Once that time is up, it comes off the Home slideshow by itself;
nothing needs to be done manually.

In the admin dashboard's **Ads** tab you can also:
- **Create an ad yourself** — runs immediately, no payment or approval step. Good for
  your own promos or a business that paid you outside the app.
- **End ad now** — stop a live ad early, before its time is up.
- **Delete** — permanently remove an ad record (works on live or past ads).

Ads are grouped into Pending approval / Live now (with time remaining shown) / History
(ended or rejected), so you can always see what's running and what's finished.

## Notes
- The Paystack **public** key is embedded in the front-end for the ad checkout — that's
  expected and safe. Never put a Paystack *secret* key in this file or any front-end code.
- `AUTH_SECRET` (optional env var): a random string used to sign login sessions. If you
  don't set one, a stable one is derived automatically from your database token, which is
  fine for most cases — set your own if you want to be able to invalidate all sessions by
  rotating it.

## 6. What the admin and students can do (newer features)

**Admin dashboard** now opens as its own full-screen page (top bar with **Exit admin**). New tabs:
- **🎬 Reels** — post a reel (a picture, or a YouTube / Shorts / .mp4 link, with a caption), delete reels, and switch on **who may post reels**.
  The same switch is on every student under **Accounts → 🎬 Reels: ON/OFF**. Switch it off at any time and that student can no longer post.
- **🎓 Courses** — build real courses: add lessons of type **Video** (YouTube link), **Slides** (upload a PDF — in PowerPoint use
  *File → Save as PDF* — or several pictures, or paste a Google Slides link), **Reading** (text), **Quiz** (multiple choice with a pass mark) or **Link**.
  Each course has a **🏆 Give a certificate** switch: ON = students get a certificate with their name when they finish; OFF = no certificate.
  Untick **Published** to keep a course as a private draft.

**Online Courses page** has two buttons: **🎬 Course Reels** (Instagram-style feed — like, comment, share to WhatsApp/Telegram/Facebook/X, copy link; double-tap a picture to like)
and **🎓 Courses** (with / without certificate). Students must log in to like, comment or get a certificate.

**Library** — a new course needs only a **name** and a **spine colour**. Inside a course, **📎 Add slides / files** opens the file picker straight away
(choose many files at once) and **📝 Add note** opens a blank note. On another device everything in the account appears — courses, notes, quizzes,
progress and notebooks follow the account — except the uploaded files themselves, which stay on the device that uploaded them (they show "☁ Added on another device"
with an **Upload here** button).

**PASCO** — **📤 Upload past questions** opens the file picker immediately (PDF, Word, PowerPoint, text or a photo). Study Partner reads the questions,
**finds the answers on the internet** with its own search engine, and makes a quiz plus an **📋 Answers** sheet showing each answer, how strong the match is, and the source.
Nothing to type. Answers are found automatically, so they can be wrong — a student can tap **✎ Set the right answer** to correct one. (Photos use a free in-browser
reader that downloads the first time it is used.)

**📓 Notes** — a small notebook window (button on the Library / PASCO / Online pages, inside every open file, and in the Research window): lined, grid or dotted paper,
handwriting fonts, blue / black / red / green / purple pen, highlighter, drawing with a finger or stylus, eraser, undo, and pages that flip. Use **📝 Save to notes** under any
research answer to drop it straight into the notebook. Notebooks sync to the account.

Reels and course content live in the same Upstash database, so they need the shared storage from section 2 to be connected.

## 7. Latest changes

- **My Desk** — tapping a book on the shelf now opens **My Desk** instead of showing the course under the shelf. It has a **‹ Library** button, the time, the study-session timer
  (the same one as on the Library page), a pinned **📌 Library rules** strip (tap to read all six rules), chips to switch between all your courses, and the course's slides/files and notes
  as clean cards that work well on a phone (filter: All · Slides & files · Notes).
- **Floating buttons** — **Library**: only the WhatsApp button. **My Desk** and **Past questions (PASCO)**: Notes 📓 (always above) and Research 🔍. **Search**: Notes only.
- **Course Reels** now work like TikTok: a full-screen feed, swipe up for the next reel, videos play by themselves, ❤ / 💬 / ↗ on the right, and a **＋** button for the admin and for students the admin has allowed.
  A reel can be a **photo**, a **short video** (up to 3.5 MB, about 10–15 seconds), or a pasted **TikTok** or **YouTube** link.
- **PASCO** — if the online answer search can't find an answer, the student can **Set answer** on any question in the Answers sheet, or use **✍ Write my own** to type their own questions and answers.
- **Shared database** — reels, courses and account sync now use their own private key names (`sp2:…`), so data from another app that uses the same Upstash database can no longer break them.
- **New logo** — two study pals sharing an open book.

### Reel videos (up to 2 minutes)

- A student picks a video; if it is **longer than 2 minutes** a **✂ Trim** screen opens to choose the part to keep (a reel can be up to 2:00).
- Videos that are **bigger than about 9 MB** are **shrunk on the phone before sending**: the chosen part is played and re-recorded small. This takes **about as long as the clip itself**
  (a 30-second clip ≈ 30 seconds), and the screen must stay open while it works — the post form shows a progress bar. Small videos upload straight away.
- Stored videos are about 2–7 MB each. They are kept in the Upstash database and sent to viewers in slices, so please keep an eye on your Upstash storage (free plan: 256 MB) if many videos are posted.
- Shrinking uses the browser's own recorder: Chrome, Edge and recent Safari work. A video that comes out as WebM (some Android phones) may not play on very old iPhones — a YouTube or TikTok link always works.
- The file viewer header now shows only **Open in new tab**, maximise and close. Notes 📓 and Research 🔍 stay available as the floating buttons.

## 8. Library, Desk, Reels and icons (latest)

- **Library** now shows only the shelf. Create a course from the top button or the drawer, then **tap the book** to open **My Desk**.
- **My Desk** has a **Back** button at the top left, the time, a **Continue learning** card (your last slide or note), the course you tapped, **My courses**, and the **Study session** timer. The WhatsApp button is hidden here; Notes and Research stay.
- **Course Reels** has **Reels | Courses** tabs at the top, the poster's profile picture on each reel, and a plain brand-colour **plus** button for posting.
  Reels open quickly: the public feed is served from the edge, the last visit is shown instantly while the new one loads, and the next reels are prepared while you watch.
- **Everything posted as a reel disappears after 30 days** (the picture or video, its comments and likes are cleared automatically). Courses, ads, library items and notes are not affected.
- **PASCO** no longer has "Write my own" — use **Answers → Set answer** to add your own answer to any question.
- The app's own screens now use clean vector icons instead of emoji, and there is a new logo (a study room: a lamp over two friends sharing a desk).


## 8. Update 3 — chats, stories, sponsored reels, past questions by school

- **Chats / Friends** (Home card or ⋮ menu): real messaging between students (and the admin). **Stories** last 24 hours.
- **Ads** now run as Sponsored reels, story circles and rotating banners in free spaces, each with **Buy now** (WhatsApp/website). They vanish when the ad expires.
- **My Desk** opens under the shelves. WhatsApp group button removed. **⋮ menu**: night/day mode, notifications, help.
- **Admin → Login background:** upload a picture or paste a video link (.mp4 / YouTube); it plays behind the glass login.
- **Admin → Past questions:** publish a PDF/photo (about 650 KB max) or a link for a school/faculty/department/level. If a student searches for one that does not exist, they see "Not Available, check in few hours" and it appears under *Students asked for these*.
- New colors are blues; admin dashboard restyled with a sidebar on wide screens.
