# Adesua Pa
Static `index.html` + one serverless function `api/index.js` (data in Upstash Redis).

## Deploy
1. Unzip, push the folder to GitHub, import it in Vercel (Framework: Other, no build command). Or run `vercel` in the folder.
2. Vercel > Settings > Environment Variables:
   - UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN (or the Upstash integration's KV_REST_API_URL / KV_REST_API_TOKEN)
   - ANTHROPIC_API_KEY (optional: smarter assistant answers, AI tutor, AI quiz questions)
   - YOUTUBE_API_KEY (optional: YouTube Data API v3 for suggested videos)
   - ADMIN_USER / ADMIN_PASS (optional; default lord / lord@123, change it)
3. Redeploy.

The assistant reads Wikipedia and DuckDuckGo on the server, so it works without ANTHROPIC_API_KEY (it then shows the source summaries directly).
Uploads are converted to PDF in the browser (PDF, DOCX, PPTX, TXT). Converted PDFs are limited to about 650 KB each.
