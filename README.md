# Adesua Pa

A static front-end (`index.html`) plus one serverless function (`server.js`). No build step — deploy as-is.

## Files

```
index.html     the whole app
manifest.json  makes the site installable as an app (name, colours, icons)
sw.js          service worker (lets the app install and open fast; never touches /api)
icons/         app icons used by the install feature (keep this folder)
voice/         optional real voice recordings for the study timer (see voice/README.txt)
logo/          logo files (SVG/PNG) for WhatsApp, posters, social media (not needed to run the site)
server.js      accounts, reels, online courses, quizzes, notifications, ads, photos, search
setup.html     open /setup after deploying — it checks that everything is connected
vercel.json    build settings + security headers
package.json
.env.example   the NAMES of the settings you add in Vercel (no values)
```

## Deploy

1. Upload the files to a **private** GitHub repository (or drag the folder into https://vercel.com/new).
2. In Vercel open **Storage** and connect an **Upstash Redis / KV** database to the project. Without it every phone keeps its own private copy of everything.
3. In **Settings → Environment Variables** add `ADMIN_USER`, `ADMIN_PASS` (or `ADMIN_USERNAME`, `ADMIN_PASSWORD` — both spellings work) and `AUTH_SECRET` (see `.env.example`). Choose your own long, unique values.
4. **Redeploy**, then open `https://YOUR-SITE.vercel.app/setup` — it shows ✅ or ❌ in plain words.

## What students see

- Bottom bar on almost every screen: Library · Activity · **Home** · Pasco · Search. Top right is the profile picture (or “Me”).
- Visitors must create an account before they can use the app.
- **Account:** photo with +, username and bio, minutes studied, certificates, courses and posts (posts only for students the admin allows), then Favorite, Ads and Notification tabs. The pencil edits the profile; the three bars open Settings (display Auto/Day/Night, change password, sign out — which always asks to confirm).
- **Library:** the shelves, then My Desk and Run business. **My Desk** holds your courses and study session.
- **Activity:** your timetable plus a Course button.
- **Course:** Reels, Courses and Friends (people you follow). Reels have like, comment, save and share; people the admin allows can post several pictures at once or one video.
- **Pasco:** Upload (open past questions in the app or turn them into an interactive quiz) and Online quiz (posted by the admin; some have gifts).

## Admin

Open the dashboard from **Me** after logging in with the admin name and password you set. Besides the existing tabs there are now **Notifications** (message everyone or one student) and **Quizzes** (post quizzes, optionally with a gift, and see who passed).

## Security notes

- Passwords are salted and hashed; sessions are signed; login attempts are rate-limited per name and per network, and accounts get a short lockout after repeated failures. Passwords must be at least 6 characters.
- The server sends security headers (HTTPS only, no framing, no content sniffing).
- Keep the repository **private**, set the admin login and `AUTH_SECRET` in Vercel (not in code), and keep payment *secret* keys out of the code and out of GitHub. The Paystack **public** key in `index.html` is meant to be public; the secret key is never needed by this app.
- If a secret was ever pasted into a chat, an email or a public repo, regenerate it in its provider's dashboard.

## Install as an app

On Chrome (Android or computer) an **Install Adesua Pa** banner appears a few seconds after a student signs in, and a purple **Get the Adesua Pa app** card sits at the top of **Account** (and in Settings) until the app is installed; Chrome's address-bar install icon and the ⋮ menu → *Install app* work too. The installed app opens full-screen from the home screen. Accounts, quizzes and payments still need internet. To release an update, just redeploy — the app picks up the newest version the next time it opens with a connection.

## Akwaaba, voice and the welcome scene

- **Welcome text** is now *Akwaaba* (Akan) everywhere.
- **Study timer voice:** the lady's message is written in Twi. Phones cannot speak real Twi by themselves, so the app plays `voice/start.mp3` and `voice/pomo.mp3` if you add recordings (best), and otherwise reads the Twi text with the closest voice the phone has. See `voice/README.txt`.
- **Library:** tapping a book on the shelf shows *Taking a seat…*, then opens My Desk. The student can tap *Return to shelf* to cancel.
- **Create account:** a short animated welcome scene (a child reading, then waving “Akwaaba!”) plays once per visit before the form. Students can tap *Skip*.
