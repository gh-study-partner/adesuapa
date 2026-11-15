# Adesua Pa
Static `index.html` + one serverless function `api/index.js` (data in Upstash Redis).

## Deploy
1. Unzip, push the folder to a GitHub repo, import it in Vercel (Framework: Other, no build command).
   (Or run `vercel` in the folder with the Vercel CLI.)
2. Vercel > Project > Settings > Environment Variables:
   - UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN (or add the Upstash integration, which sets KV_REST_API_URL / KV_REST_API_TOKEN)
   - ANTHROPIC_API_KEY (optional: AI tutor + AI quiz questions)
   - YOUTUBE_API_KEY (optional: YouTube Data API v3 for video search)
   - ADMIN_USER / ADMIN_PASS (optional: default is lord / lord@123; change it)
3. Redeploy.

PDF uploads are limited to about 650 KB each (Upstash request limit).
