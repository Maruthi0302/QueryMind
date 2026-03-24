"""
============================================================
QueryMind AI — Real-Time Inbound Voice Agent
============================================================
File:    agent.py
Python:  3.10+
Framework: livekit-agents v1.4.x

WHAT THIS FILE DOES
--------------------
Entry point for the voice agent. Run with:
    python agent.py dev

The agent:
  1. Connects to a LiveKit room (where Twilio SIP sends the call)
  2. Listens to the caller using Silero VAD + Deepgram STT
  3. Sends each transcribed utterance to Groq (Llama 3.3 70B)
  4. Streams the LLM tokens directly into ElevenLabs TTS
  5. Speaks back — while supporting barge-in interruption
  6. When the call ends, logs everything to Airtable

STREAMING PIPELINE
------------------
Caller mic
  → Silero VAD          (detects when speech starts/ends)
  → Deepgram STT        (converts audio to text, streaming)
  → Groq LLM            (generates response, streaming tokens)
  → ElevenLabs TTS      (converts text to audio, streaming)
  → Caller speaker

NOTE: All plugin API parameters in this file have been verified
against the actually-installed package versions:
  livekit-agents==1.4.4
  livekit-plugins-silero==1.4.4    (VAD.load uses activation_threshold)
  livekit-plugins-deepgram==1.4.4  (STT uses api_key directly)
  livekit-plugins-groq==1.4.4     (LLM uses max_completion_tokens)
  livekit-plugins-elevenlabs==1.4.4 (TTS uses api_key directly)
"""

# ============================================================
# SECTION 1 — Standard Library Imports
# ============================================================
import asyncio
import logging
import os
import signal
from dataclasses import dataclass, field
from datetime import datetime, timezone

# ============================================================
# SECTION 2 — Third-Party Imports
# ============================================================
import aiohttp
from dotenv import load_dotenv
from twilio.request_validator import RequestValidator

# LiveKit Agents v1.x — verified working imports
from livekit.agents import Agent, AgentSession, JobContext, WorkerOptions, cli
from livekit.plugins import cartesia, deepgram, groq, silero

# ============================================================
# [KOKORO FALLBACK — commented out, uncomment to use free TTS]
# ============================================================
# If ElevenLabs quota runs out, switch to Kokoro (free, self-hosted):
#   1. pip install kokoro-onnx soundfile
#   2. Download ONNX model from HuggingFace/kokoro-tts
#   3. Uncomment: from livekit.plugins import kokoro
#   4. In entrypoint(), replace the elevenlabs.TTS(...) block with:
#        tts = kokoro.TTS(model_path="./kokoro-v0_19.onnx", voice="af")

# ============================================================
# SECTION 3 — Load Environment Variables
# ============================================================
load_dotenv()

# ============================================================
# SECTION 4 — Structured Logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("querymind.agent")

# ============================================================
# SECTION 5 — Configuration from Environment
# ============================================================
LIVEKIT_URL        = os.getenv("LIVEKIT_URL", "")
LIVEKIT_API_KEY    = os.getenv("LIVEKIT_API_KEY", "")
LIVEKIT_API_SECRET = os.getenv("LIVEKIT_API_SECRET", "")

CARTESIA_API_KEY   = os.getenv("CARTESIA_API_KEY", "")
CARTESIA_VOICE_ID  = os.getenv("CARTESIA_VOICE_ID", "e07c00bc-4134-4eae-9ea4-1a55fb45746b")

DEEPGRAM_API_KEY    = os.getenv("DEEPGRAM_API_KEY", "")
GROQ_API_KEY        = os.getenv("GROQ_API_KEY", "")

TWILIO_ACCOUNT_SID  = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN   = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER", "")

AIRTABLE_PAT        = os.getenv("AIRTABLE_PAT", "")
AIRTABLE_BASE_ID    = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "call_logs")

# Calls exceeding this are gracefully ended (prevents runaway API costs)
MAX_CALL_DURATION_SECONDS = int(os.getenv("MAX_CALL_DURATION_SECONDS", "300"))

# Fail fast at startup if any required key is missing
REQUIRED_ENV_VARS = {
    "LIVEKIT_URL": LIVEKIT_URL,
    "LIVEKIT_API_KEY": LIVEKIT_API_KEY,
    "LIVEKIT_API_SECRET": LIVEKIT_API_SECRET,
    "DEEPGRAM_API_KEY": DEEPGRAM_API_KEY,
    "GROQ_API_KEY": GROQ_API_KEY,
    "CARTESIA_API_KEY": CARTESIA_API_KEY,
    "TWILIO_ACCOUNT_SID": TWILIO_ACCOUNT_SID,
    "TWILIO_AUTH_TOKEN": TWILIO_AUTH_TOKEN,
    "AIRTABLE_PAT": AIRTABLE_PAT,
    "AIRTABLE_BASE_ID": AIRTABLE_BASE_ID,
}
for var_name, var_value in REQUIRED_ENV_VARS.items():
    if not var_value:
        raise EnvironmentError(
            f"Missing required environment variable: {var_name}\n"
            "Copy .env.example → .env and fill in your credentials."
        )

logger.info("All required environment variables loaded ✓")

# ============================================================
# SECTION 6 — Call Session State
# ============================================================
@dataclass
class CallSession:
    """
    In-memory state for one phone call.

    caller_number    : E.164 number of the caller (e.g. "+917569669019")
    call_start_time  : UTC datetime when the call was connected
    transcript_buffer: List of final STT utterances, joined at call end
    conversation_context: Reserved for future use
    """
    caller_number:        str = "unknown"
    call_start_time:      datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    transcript_buffer:    list[str] = field(default_factory=list)
    conversation_context: dict = field(default_factory=dict)

    def elapsed_seconds(self) -> float:
        return (datetime.now(timezone.utc) - self.call_start_time).total_seconds()

    def full_transcript(self) -> str:
        return " ".join(self.transcript_buffer).strip()


# ============================================================
# SECTION 7 — Airtable Logging
# ============================================================
# Uses aiohttp (async) — never blocks the event loop.
# Fully wrapped in try/except — a logging failure NEVER crashes the call.

async def log_call_to_airtable(session: CallSession) -> None:
    """
    Insert a completed call record into Airtable.
    Fields: caller_number, duration_seconds, transcript, created_at
    This function NEVER raises exceptions.
    """
    try:
        duration   = round(session.elapsed_seconds(), 1)
        transcript = session.full_transcript() or "(no speech detected)"
        created_at = datetime.now(timezone.utc).isoformat()

        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
        headers = {
            "Authorization": f"Bearer {AIRTABLE_PAT}",
            "Content-Type": "application/json",
        }
        payload = {
            "fields": {
                "caller_number":    session.caller_number,
                "duration_seconds": duration,
                "transcript":       transcript,
                "created_at":       created_at,
            }
        }

        logger.info(
            f"Logging to Airtable | caller={session.caller_number} "
            f"| duration={duration}s | chars={len(transcript)}"
        )

        async with aiohttp.ClientSession() as http:
            async with http.post(url, json=payload, headers=headers) as resp:
                if resp.status in (200, 201):
                    logger.info("Airtable: record inserted ✓")
                else:
                    body = await resp.text()
                    logger.error(f"Airtable: status {resp.status} | {body}")

    except aiohttp.ClientError as e:
        logger.error(f"Airtable: network error — {e}")
    except Exception as e:
        logger.error(f"Airtable: unexpected error — {type(e).__name__}: {e}")


# ============================================================
# SECTION 8 — Twilio Webhook Signature Validation
# ============================================================
def validate_twilio_request(url: str, post_params: dict, signature: str) -> bool:
    """
    Verify a request genuinely came from Twilio.
    Returns True if authentic, False if spoofed.

    Usage (FastAPI example):
        sig = request.headers.get("X-Twilio-Signature", "")
        if not validate_twilio_request(str(request.url), dict(form), sig):
            raise HTTPException(403, "Invalid Twilio signature")
    """
    validator = RequestValidator(TWILIO_AUTH_TOKEN)
    is_valid = validator.validate(url, post_params, signature)
    if not is_valid:
        logger.warning(f"Twilio signature FAILED for: {url}")
    return is_valid


# ============================================================
# SECTION 9 — VAD Configuration (Silero v1.4.4)
# ============================================================
# Verified parameters from silero.VAD.load() in livekit-plugins-silero v1.4.4:
#
#   activation_threshold  : Probability (0.0–1.0) above which audio = "speech".
#                           Higher = less sensitive. Default: 0.5
#                           Increase if background noise triggers false starts.
#                           Decrease if soft-spoken callers are missed.
#
#   min_silence_duration  : Seconds of silence after last word before the
#                           utterance is considered complete. Default: 0.55s
#                           Increase if callers speak slowly (e.g. 0.8)
#                           Decrease to speed up turn-taking (e.g. 0.4)
#
#   prefix_padding_duration: Extra audio buffer before the speech segment.
#                           Prevents the first word from being clipped.
#                           Default: 0.5s — fine for most voices.

def build_vad() -> silero.VAD:
    """Create and return a configured Silero VAD instance."""
    return silero.VAD.load(
        activation_threshold=0.5,      # probability threshold for speech detection
        min_silence_duration=0.55,     # seconds of silence before turn ends
        prefix_padding_duration=0.5,   # buffer before speech segment starts
    )


# ============================================================
# SECTION 10 — System Prompt
# ============================================================
SYSTEM_PROMPT = """
You are a helpful voice assistant called QueryMind.
You are speaking on a phone call.
Keep ALL responses short, natural, and conversational — 
no bullet points, no markdown, no long paragraphs.
Speak in a friendly, clear tone.
If you don't know something, say so honestly in one sentence.
""".strip()


# ============================================================
# SECTION 11 — QueryMind Agent Class
# ============================================================
# In livekit-agents v1.x, Agent is subclassed to define behavior.
# The framework handles VAD → STT → LLM → TTS automatically.
# We override on_user_turn_completed to buffer transcripts.

class QueryMindAgent(Agent):
    """
    The QueryMind voice agent brain.

    Subclasses livekit's Agent which handles:
    - Receiving STT transcripts
    - Querying the LLM with conversation history
    - Streaming TTS output back to caller
    - Barge-in (via allow_interruptions in AgentSession)

    We capture each user utterance here for Airtable logging.
    """

    def __init__(self, session_state: CallSession) -> None:
        super().__init__(instructions=SYSTEM_PROMPT)
        self.session_state = session_state

    async def on_user_turn_completed(self, turn_ctx, new_message) -> None:
        """
        Called after each finalized user utterance.
        Captures the transcript text for later Airtable logging.
        Always calls super() to keep the framework running normally.
        """
        try:
            text = ""
            if hasattr(new_message, "content"):
                content = new_message.content
                if isinstance(content, str):
                    text = content
                elif isinstance(content, list):
                    parts = []
                    for block in content:
                        if hasattr(block, "text"):
                            parts.append(block.text)
                        elif isinstance(block, str):
                            parts.append(block)
                    text = " ".join(parts)
            if text:
                self.session_state.transcript_buffer.append(text)
                logger.info(f"Transcript captured: '{text}'")
        except Exception as e:
            logger.warning(f"Could not capture transcript: {e}")

        # CRITICAL: always call super() so the framework continues
        await super().on_user_turn_completed(turn_ctx, new_message)


# ============================================================
# SECTION 12 — Main Agent Entrypoint
# ============================================================
# LiveKit calls this once per inbound call as an async task.

async def entrypoint(ctx: JobContext) -> None:
    """
    Main handler for each inbound phone call.

    Steps:
        1. Connect to the LiveKit room
        2. Extract caller phone number
        3. Create call session state
        4. Build STT, LLM, TTS, VAD with verified API signatures
        5. Create AgentSession (wires the pipeline + handles barge-in)
        6. Start the agent and speak greeting
        7. Start max-duration watchdog
        8. Wait for call to end, then log to Airtable
    """

    # Step 1: Connect to the LiveKit room
    logger.info(f"New inbound call | room={ctx.room.name}")
    await ctx.connect()

    # Step 2: Extract caller phone number
    # Room name from Twilio SIP looks like: call-_+917569669019_WDX9LAtRGMJV
    # We parse the caller number from it first, then try participant identity.
    caller_number = "unknown"
    try:
        room_name = ctx.room.name
        if "call-_" in room_name:
            parts = room_name.split("_")
            if len(parts) > 1:
                caller_number = parts[1]

        if caller_number == "unknown":
            for participant in ctx.room.remote_participants.values():
                identity = participant.identity or ""
                if "@" in identity:
                    caller_number = identity.split("@")[0]
                elif identity.startswith("+"):
                    caller_number = identity
                if caller_number != "unknown":
                    break
    except Exception as e:
        logger.warning(f"Could not extract caller number: {e}")

    logger.info(f"Caller: {caller_number} | max_duration: {MAX_CALL_DURATION_SECONDS}s")

    # Step 3: Create call session
    session_state = CallSession(caller_number=caller_number)

    # Step 4a: Deepgram STT
    # Verified params: model, language, smart_format, interim_results, api_key
    # nova-3 is the latest default model in the installed plugin version
    stt = deepgram.STT(
        model="nova-3",
        language="en-US",
        smart_format=True,      # auto punctuation + number formatting
        interim_results=True,   # get partial transcripts in real time
        api_key=DEEPGRAM_API_KEY,
    )

    # Step 4b: Groq LLM (Llama 3.3 70B)
    # Verified params: model, temperature, max_completion_tokens, api_key
    # max_completion_tokens=150 keeps responses short and low-latency
    llm = groq.LLM(
        model="llama-3.3-70b-versatile",
        temperature=0.7,
        max_completion_tokens=150,
        api_key=GROQ_API_KEY,
    )

    # Step 4c: Cartesia TTS (sonic-3 — human-like, 40ms latency, free tier)
    # Using the user-selected voice via CARTESIA_VOICE_ID.
    # sonic-3 is Cartesia's flagship model: multilingual, ultra-low latency.
    tts = cartesia.TTS(
        model="sonic-2",
        voice=CARTESIA_VOICE_ID,
        api_key=CARTESIA_API_KEY,
        language="en",
    )

    # Step 4d: Silero VAD
    # Verified params: activation_threshold, min_silence_duration, prefix_padding_duration
    vad = build_vad()

    # Step 5: Create AgentSession
    # This is the core pipeline: VAD → STT → Agent(LLM) → TTS
    #
    # allow_interruptions=True  → barge-in is automatic. If the caller speaks
    #                             while TTS is playing, the framework cancels
    #                             TTS + LLM immediately and resumes listening.
    #
    # min_endpointing_delay    → minimum silence before turn ends (seconds)
    # max_endpointing_delay    → maximum wait after silence before forcing turn end
    agent_session = AgentSession(
        vad=vad,
        stt=stt,
        llm=llm,
        tts=tts,
        allow_interruptions=True,
        min_endpointing_delay=0.7,
        max_endpointing_delay=3.0,
    )

    # Step 6: Hook into session events for observability logging
    @agent_session.on("agent_speech_interrupted")
    def on_interrupted(_event) -> None:
        logger.info("Barge-in: agent speech interrupted by caller")

    @agent_session.on("agent_started_speaking")
    def on_speaking_start(_event) -> None:
        logger.info("Agent: started speaking (TTS streaming)")

    @agent_session.on("agent_stopped_speaking")
    def on_speaking_stop(_event) -> None:
        logger.info("Agent: finished speaking")

    # Step 6b: Start the agent session
    await agent_session.start(
        agent=QueryMindAgent(session_state=session_state),
        room=ctx.room,
    )

    # Greet the caller immediately
    await agent_session.say(
        "Hi! You've reached QueryMind AI. How can I help you today?",
        allow_interruptions=True,
    )

    logger.info("Agent is now live and listening ✓")

    # Step 7: Max-duration watchdog
    # Sleeps for MAX_CALL_DURATION_SECONDS then gracefully disconnects.
    # Prevents runaway API costs from excessively long calls.
    async def max_duration_watchdog() -> None:
        await asyncio.sleep(MAX_CALL_DURATION_SECONDS)
        logger.warning(
            f"Max call duration ({MAX_CALL_DURATION_SECONDS}s) reached "
            f"— disconnecting {session_state.caller_number}"
        )
        try:
            await agent_session.say(
                "This call has reached its maximum duration. Thank you for calling QueryMind!"
            )
        except Exception:
            pass
        await ctx.room.disconnect()

    watchdog_task = asyncio.create_task(max_duration_watchdog())

    # Step 8: Register a shutdown callback for cleanup + Airtable logging
    # In v1.4.4, ctx.add_shutdown_callback() fires when the session ends
    # (caller hangs up, network drops, or watchdog fires).
    async def on_shutdown() -> None:
        # Cancel the watchdog if the call ended before the time limit
        watchdog_task.cancel()
        try:
            await watchdog_task
        except asyncio.CancelledError:
            pass

        duration = round(session_state.elapsed_seconds(), 1)
        logger.info(
            f"Call ended | caller={session_state.caller_number} "
            f"| duration={duration}s | turns={len(session_state.transcript_buffer)}"
        )
        await log_call_to_airtable(session_state)

    ctx.add_shutdown_callback(on_shutdown)

    # Step 9: Wait for the session to close.
    # AgentSession emits a "close" event when the caller hangs up,
    # the room disconnects, or the watchdog fires.
    # We use asyncio.Event to block the entrypoint cleanly.
    session_done = asyncio.Event()

    @agent_session.on("close")
    def on_session_close(_event) -> None:
        session_done.set()

    await session_done.wait()


# ============================================================
# SECTION 13 — Graceful Shutdown
# ============================================================
def handle_shutdown(signum, frame) -> None:
    logger.info(f"Shutdown signal {signum} received — stopping agent…")
    raise SystemExit(0)

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# ============================================================
# SECTION 14 — Main Entry Point
# ============================================================
# python agent.py dev      ← development (verbose, auto-reload)
# python agent.py start    ← production
# python agent.py console  ← terminal test without a real phone

if __name__ == "__main__":
    logger.info("Starting QueryMind AI Voice Agent…")
    logger.info(f"LiveKit: {LIVEKIT_URL}")
    logger.info(f"Max call duration: {MAX_CALL_DURATION_SECONDS}s")
    logger.info(f"Cartesia TTS | voice: {CARTESIA_VOICE_ID} | model: sonic-2")

    cli.run_app(
        WorkerOptions(
            entrypoint_fnc=entrypoint,
            agent_name="querymind",
        )
    )
