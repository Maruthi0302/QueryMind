# QueryMind AI — Real-Time Inbound Voice Agent

A production-style voice AI agent that answers inbound phone calls, holds a natural conversation powered by Llama 3.3 70B, and logs every call to Airtable.

**Stack:** LiveKit · Deepgram STT · Groq LLM · ElevenLabs TTS · Silero VAD · Twilio SIP · Airtable

---

## What It Does

| Feature | Detail |
|---------|--------|
| Answers inbound calls | Via Twilio SIP → LiveKit room |
| Streaming STT | Deepgram Nova-2, real-time transcription |
| Streaming LLM | Groq Llama 3.3 70B, tokens stream as they arrive |
| Streaming TTS | ElevenLabs Turbo v2, agent starts speaking within ~300ms |
| Barge-in support | Caller can interrupt the agent mid-sentence |
| VAD | Silero VAD — tunable sensitivity |
| Max duration | Auto-ends calls after `MAX_CALL_DURATION_SECONDS` |
| Call logging | Logs call to Airtable: number, duration, transcript, timestamp |

---

## Prerequisites

- Python 3.10 or higher
- A free account on each service listed below
- ngrok or a public VPS (so Twilio can reach your machine)

---

## 1. Install Dependencies

```bash
# Create a virtual environment (recommended)
python -m venv .venv

# Activate it
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

# Install all packages
pip install -r requirements.txt
```

---

## 2. Configure Environment Variables

```bash
# Copy the example file
copy .env.example .env    # Windows
cp .env.example .env      # macOS/Linux

# Now open .env and fill in your credentials
```

### Where to Get Each API Key

| Variable | Service | Link |
|----------|---------|------|
| `LIVEKIT_URL` / `LIVEKIT_API_KEY` / `LIVEKIT_API_SECRET` | LiveKit Cloud | [cloud.livekit.io](https://cloud.livekit.io) → New Project → Settings → Keys |
| `DEEPGRAM_API_KEY` | Deepgram | [console.deepgram.com](https://console.deepgram.com) → API Keys |
| `GROQ_API_KEY` | Groq | [console.groq.com](https://console.groq.com) → API Keys |
| `ELEVENLABS_API_KEY` | ElevenLabs | [elevenlabs.io](https://elevenlabs.io) → Profile → API Keys |
| `ELEVENLABS_VOICE_ID` | ElevenLabs | Voices page → click a voice → copy its ID |
| `TWILIO_ACCOUNT_SID` / `TWILIO_AUTH_TOKEN` | Twilio | [console.twilio.com](https://console.twilio.com) → Dashboard home |
| `TWILIO_PHONE_NUMBER` | Twilio | Console → Phone Numbers → Active Numbers |
| `AIRTABLE_PAT` | Airtable | Account → Developer Hub → Personal Access Tokens |
| `AIRTABLE_BASE_ID` | Airtable | Open base → Help → API → Base ID starts with `app` |

---

## 3. Set Up Airtable

1. Create a base at [airtable.com](https://airtable.com)
2. Create a table named **`call_logs`** (must match `AIRTABLE_TABLE_NAME` in `.env`)
3. Add these fields exactly:

| Field Name | Field Type |
|-----------|-----------|
| `caller_number` | Single line text |
| `duration_seconds` | Number |
| `transcript` | Long text |
| `created_at` | Single line text |

---

## 4. Connect Twilio → LiveKit (SIP Bridge)

This is how a real phone call reaches your Python agent:

```
Caller dials your Twilio number
  → Twilio routes via SIP Trunk → LiveKit SIP Server
  → LiveKit creates a room → dispatches a job → agent.py entrypoint() is called
```

### Step-by-step

1. **LiveKit SIP Server** — In LiveKit Cloud console, go to **SIP** and create an **Inbound SIP Trunk**. Copy the SIP URI (e.g., `sip.livekit.cloud`)

2. **Twilio SIP Trunk** — In Twilio console:
   - Go to **Elastic SIP Trunking** → **Trunks** → Create
   - Under **Origination**, add the LiveKit SIP URI as an Origination SIP URI
   - Under **Numbers**, attach your Twilio phone number to this trunk

3. **Dispatch Rule** — In LiveKit Cloud SIP settings, create a **Dispatch Rule** that maps inbound calls to a room name of your choice

4. That's it — when a caller dials your Twilio number, the call flows through to your agent automatically

---

## 5. Run the Agent

```bash
# Development mode (verbose, auto-connects to LiveKit)
python agent.py dev

# Production mode
python agent.py start

# Test without a real phone — simulates a conversation in your terminal
python agent.py console
```

You should see logs like:
```
2026-03-07 23:30:00 | INFO     | querymind.agent | All required environment variables loaded ✓
2026-03-07 23:30:01 | INFO     | querymind.agent | Starting QueryMind AI Voice Agent…
2026-03-07 23:30:01 | INFO     | querymind.agent | LiveKit server: wss://your-project.livekit.cloud
```

---

## 6. Pre-Flight Checklist

Run through this before your demo:

- [ ] Python 3.10+ installed (`python --version`)
- [ ] Virtual environment created and activated
- [ ] `pip install -r requirements.txt` completed with no errors
- [ ] `.env` file created and all values filled in
- [ ] LiveKit project created, URL + Key + Secret copied to `.env`
- [ ] LiveKit SIP Inbound Trunk created
- [ ] Deepgram account created, API key added to `.env`
- [ ] Groq account created, API key added to `.env`
- [ ] ElevenLabs account created, API key + Voice ID added to `.env`
- [ ] Twilio trial account active, phone number purchased
- [ ] Twilio SIP Trunk connected to LiveKit SIP URI
- [ ] Twilio phone number attached to the SIP Trunk
- [ ] Airtable base + `call_logs` table created with correct field names
- [ ] Airtable PAT + Base ID added to `.env`
- [ ] `python agent.py dev` starts with no errors
- [ ] Test call made — agent picks up and responds
- [ ] Airtable row created after call ends

---

## VAD Tuning

If callers are being cut off mid-sentence or the agent is slow to respond, adjust these values in `agent.py` → **`build_vad()`**:

| Parameter | Effect | Increase if… | Decrease if… |
|-----------|--------|-------------|-------------|
| `speech_threshold` | Sensitivity | Too many false triggers | Missing quiet speech |
| `min_silence_duration` | Wait after speech | Callers speak slowly | Agent is slow to respond |
| `speech_pad_ms` | Buffer around speech | First/last word clipped | Too much silence included |

---

## Switching to Kokoro TTS (Free, Self-Hosted)

If ElevenLabs quota runs out:

1. `pip install kokoro-onnx soundfile`
2. Download the ONNX model from [HuggingFace/kokoro](https://huggingface.co/kokoro-tts)
3. In `agent.py`, uncomment the Kokoro import and TTS line, comment out ElevenLabs

---

## Project Structure

```
QueryMind AI/
├── agent.py          ← Main agent (all logic lives here)
├── requirements.txt  ← Python dependencies
├── .env.example      ← Template for credentials
├── .env              ← Your real credentials (git-ignored)
├── .gitignore        ← Never commit .env
└── README.md         ← This file
```
