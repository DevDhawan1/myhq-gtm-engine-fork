# Apollo Webhook Receiver

FastAPI service that buffers Apollo async phone reveals into Postgres.
Deployed on Render free tier; consumed by `pipeline/apollo_reconciler.py`.

## Deploy to Render

1. Push this repo to GitHub (if not already).
2. Sign up / log in at https://render.com.
3. Dashboard → **New** → **Blueprint** → pick this repo → Render detects `webhook_server/render.yaml`.
4. When prompted, set the env vars (values come from your local `.env`):
   - `DATABASE_URL` — Supabase Postgres URI
   - `APOLLO_WEBHOOK_SECRET` — the random string you generated
5. Click **Apply** → wait ~3 min for first deploy.
6. Note your service URL (looks like `https://apollo-webhook-xxxx.onrender.com`).

## Wire it into Apollo

Update `.env` in the main project:

```
APOLLO_WEBHOOK_URL=https://apollo-webhook-xxxx.onrender.com/apollo/webhook?t=YOUR_SECRET
```

## Keep-warm (prevents Render cold starts)

Free tier spins down after 15 min idle. Add a UptimeRobot monitor:

1. Sign up at https://uptimerobot.com (free).
2. **Add New Monitor** → type: HTTP(s) → URL: `https://apollo-webhook-xxxx.onrender.com/health` → interval: 5 min.
3. Save.

## Local testing

```bash
cd webhook_server
pip install -r requirements.txt
export DATABASE_URL=...
export APOLLO_WEBHOOK_SECRET=...
uvicorn app:app --reload --port 8000
```

Health check:
```bash
curl http://localhost:8000/health
```
