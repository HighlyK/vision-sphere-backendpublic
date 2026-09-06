# Vision Sphere — Backend
Geospatial Intelligence Processing Engine  
English + Burmese Documentation

---

## 1. Introduction (English)

Vision Sphere Backend is the core engine that powers the Vision Sphere platform.  
It handles global data ingestion, conflict mapping, geospatial processing, and API delivery for the frontend globe.  
The backend is designed to be modular, stable, and extendable.

---

## 1. မိတ်ဆက် (မြန်မာ)

Vision Sphere Backend က Vision Sphere Platform တစ်ခုလုံးကို လည်ပတ်စေတဲ့ အခြေခံအင်ဂျင် ဖြစ်ပါတယ်။  
ကမ္ဘာလုံးဆိုင်ရာ ဒေတာ layer တွေ၊ ပြင်းထန်မှု map တွေ၊ geospatial process တွေကို backend က စနစ်တကျ ပြုလုပ်ပေးပါတယ်။  
Module အလိုက် ခွဲထားပြီး တိုးချဲ့ရလွယ်ကူအောင် ဒီဇိုင်းလုပ်ထားပါတယ်။

---

## 2. Features (English)

- Global data layer ingestion  
- Conflict event processing and clustering  
- Geospatial pipelines (boundaries, region detection, transforms)  
- REST API for frontend globe  
- Database storage (Supabase / Turso)  
- Cloudflare Worker proxy integration  
- Scheduled updates (cron jobs)  
- Layer normalization and rendering output  

---

## 2. လက္ခဏာများ (မြန်မာ)

- ကမ္ဘာလုံးဆိုင်ရာ ဒေတာ layer တွေကို စုဆောင်းခြင်း  
- ပြင်းထန်မှု data တွေကို process လုပ်ပြီး cluster ချခြင်း  
- Geospatial pipeline (နယ်မြေကန့်သတ်, region detect, coordinate transform)  
- Frontend globe အတွက် REST API  
- Database storage (Supabase / Turso)  
- Cloudflare Worker proxy  
- Cron job update  
- Layer normalization and rendering output  

---

## 3. Tech Stack

- Node.js / TypeScript  
- Supabase / PostgreSQL  
- Turso (SQLite Edge)  
- Cloudflare Workers  
- REST API  
- GeoJSON / Turf.js  
- Cron Jobs  

---

## 4. Project Structure

visionsphere-backend/
│
├── src/
│   ├── api/              # REST endpoints
│   ├── layers/           # Global data layer processors
│   ├── conflict/         # Conflict ingestion and mapping
│   ├── geospatial/       # Region detection and transforms
│   ├── workers/          # Cloudflare worker logic
│   └── utils/            # Shared utilities
│
├── database/             # Schema and migrations
├── scripts/              # Automation scripts
├── docs/                 # Documentation
├── .env.example          # Environment variable template
└── README.md

Code

---

## 5. Setup

git clone <repo-url>
cd visionsphere-backend
npm install
cp .env.example .env
npm run dev

Code

---

## 6. Data Layers (English)

Each global layer includes:

- Fetcher  
- Processor  
- Normalizer  
- Storage handler  
- Renderer output  

Examples:

- FIRMS fire data  
- Weather layers  
- Population density  
- Geopolitical boundaries  
- Custom intelligence layers  

API:

GET /api/layers
GET /api/layers/:id

Code

---

## 6. Data Layers (မြန်မာ)

Layer တစ်ခုချင်းစီမှာ—

- fetcher  
- processor  
- normalizer  
- storage  
- renderer output  

တွေ ပါပါတယ်။

API:

GET /api/layers
GET /api/layers/:id

Code

---

## 7. Conflict Mapping Engine (English)

The conflict engine handles:

- ACLED conflict ingestion  
- Severity scoring  
- Cluster detection  
- Region summaries  
- Timeline grouping  

API:

GET /api/conflict
GET /api/conflict/summary

Code

---

## 7. Conflict Mapping (မြန်မာ)

Conflict engine က—

- ACLED data  
- severity score  
- cluster detect  
- region summary  
- timeline  

တွေကို process လုပ်ပါတယ်။

API:

GET /api/conflict
GET /api/conflict/summary

Code

---

## 8. Deployment

Vision Sphere Backend can run on:

- Node.js servers  
- Cloudflare Workers  
- Supabase Edge Functions  

Environment variables are stored in `.env`.

---

## 9. License

MIT License (or your preferred license).