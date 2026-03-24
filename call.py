"""
============================================================
QueryMind AI — Outbound Call Trigger
============================================================
File: call.py

USAGE:
    python call.py +917XXXXXXXXX

HOW IT WORKS:
    1. This script tells Twilio to CALL the given number
    2. When they pick up, Twilio plays a brief message
       then bridges them into LiveKit SIP
    3. Your running agent.py picks up the LiveKit job
    4. The caller hears your AI agent immediately

REQUIREMENTS:
    - agent.py must be running in another terminal
    - The destination number must be a Twilio Verified Caller ID
      (Twilio Console → Phone Numbers → Verified Caller IDs)
    - Your .env file must be filled in
"""

import sys
import os
from dotenv import load_dotenv
from twilio.rest import Client

# Load credentials from .env
load_dotenv()

TWILIO_ACCOUNT_SID  = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN   = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")

# The LiveKit SIP URI for your project
# (Found in LiveKit Cloud → Settings → Project → SIP URI)
LIVEKIT_SIP_URI = "2eidjosxo7s.sip.livekit.cloud"

def make_outbound_call(to_number: str) -> None:
    """
    Tell Twilio to call `to_number` and bridge them into LiveKit.

    Args:
        to_number: E.164 format, e.g. "+917569669019"
    """
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
        print("ERROR: Missing Twilio credentials in .env file")
        sys.exit(1)

    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

    # TwiML that Twilio executes once the callee picks up:
    # It immediately dials out to the LiveKit SIP URI,
    # which dispatches the job to your running agent.
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Dial>
        <Sip>sip:{TWILIO_PHONE_NUMBER.replace('+', '')}@{LIVEKIT_SIP_URI}</Sip>
    </Dial>
</Response>"""

    print(f"📞 Calling {to_number} via Twilio...")
    print(f"   From: {TWILIO_PHONE_NUMBER}")
    print(f"   Bridging to LiveKit SIP: {LIVEKIT_SIP_URI}")
    print()

    try:
        call = client.calls.create(
            to=to_number,
            from_=TWILIO_PHONE_NUMBER,
            twiml=twiml,
        )
        print(f"✅ Call initiated! SID: {call.sid}")
        print(f"   Status: {call.status}")
        print()
        print("The person's phone is ringing now.")
        print("When they pick up, your QueryMind AI agent will speak to them.")
        print()
        print("Watch your agent.py terminal for:")
        print("  INFO | New inbound call | room=call-...")
        print("  INFO | Agent is now live and listening ✓")

    except Exception as e:
        print(f"❌ Call failed: {e}")
        print()
        print("Common reasons:")
        print("  - The destination number is not a Twilio Verified Caller ID")
        print("    → Fix: Twilio Console → Phone Numbers → Verified Caller IDs")
        print("  - Your Twilio trial credit is exhausted")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python call.py <phone_number>")
        print("Example: python call.py +917569669019")
        print()
        print("NOTE: The number must be verified in Twilio Console:")
        print("  Twilio Console → Phone Numbers → Verified Caller IDs")
        sys.exit(1)

    to_number = sys.argv[1]

    # Basic E.164 validation
    if not to_number.startswith("+"):
        print(f"ERROR: Number must be in E.164 format, e.g. +917569669019")
        print(f"You entered: {to_number}")
        sys.exit(1)

    make_outbound_call(to_number)
