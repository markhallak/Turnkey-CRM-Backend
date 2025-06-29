import asyncio
import json
import os

import asyncpg
import requests

from constants import ASYNCPG_URL

API_KEY = "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"
BREVO_URL = "https://api.brevo.com/v3/smtp/email"

# Channels we LISTEN to in Postgres
NOTIFICATION_CHANNEL = "new_notification_row"
MAGIC_LINK_CHANNEL = "new_magic_link_row"


async def send_email(subject: str, body: str, recipient: str):
    headers = {
        "accept": "application/json",
        "api-key": API_KEY,
        "content-type": "application/json",
    }

    payload = {
        "sender": {
            "name": "TurnKey Consultancy",
            "email": "no-reply@turnkey.com"
        },
        "to": [
            {
                "email": recipient,
                "name": "User"
            }
        ],
        "subject": subject,
        "htmlContent": body
    }

    response = requests.post(BREVO_URL, headers=headers, json=payload)

    if response.ok:
        print("Email sent successfully!")
        print("Response:", response.json())
        return True
    else:
        print(f"Error {response.status_code}: {response.text}")
        return False


async def handle_notification(conn, pid, channel, payload):
    """
    Handler for INSERTs on the notification table.
    Payload is the full row as a JSON string.
    """
    data = json.loads(payload)
    # Try to look up the user email based on triggered_by_id
    recipient = None
    trigger_id = data.get("triggered_by_id")
    if trigger_id:
        user = await conn.fetchrow('SELECT email FROM "user" WHERE id = $1', trigger_id)
        if user:
            recipient = user["email"]

    if not recipient:
        print(f"[notification] no recipient found for notification {data.get('id')}")
        return

    subject = f"New Notification: {data.get('triggered_by_category')}"
    body = (
        f"You have a new notification:\n\n"
        f"Content: {data.get('content')}\n"
        f"Created at: {data.get('created_at')}\n\n"
        f"Full payload:\n{json.dumps(data, indent=2)}"
    )
    await send_email(subject, body, recipient)


async def handle_magic_link(connection, pid, channel, payload):
    data = json.loads(payload)
    recipient = data.get("send_to")

    if not recipient:
        print(f"[magic_link] no send_to in payload: {data}")
        return

    purpose = data.get("purpose")

    if purpose == "signup":
        subject = "Your Magic Login Link"

        if data.get("uuid") and data.get("sig"):
            frontend_base = os.getenv("FRONTEND_BASE_URL", "http://localhost:8000")
            link = f"{frontend_base}/auth/sign-up?uuid={data['uuid']}&sig={data['sig']}"

            htmlBody = f"""
            <!DOCTYPE html>
            <html lang="en">
              <head>
                <meta charset="UTF-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
                <style>
                  /* Reset & base */
                  body, table, td, a {{ -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }}
                  table {{ border-collapse: collapse !important; }}
                  img {{ border: 0; height: auto; line-height: 100%; outline: none; text-decoration: none; }}
                  body {{ margin: 0 !important; padding: 0 !important; width: 100% !important; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; background-color: #f4f4f4; }}

                  /* Container */
                  .email-container {{
                    max-width: 600px;
                    margin: 0 auto;
                    background-color: #ffffff;
                    border-radius: 8px;
                    overflow: hidden;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                  }}
                  .header {{
                    padding: 20px;
                    background-color: #4a90e2;
                    text-align: center;
                    color: #ffffff;
                  }}
                  .header h1 {{
                    margin: 0;
                    font-size: 24px;
                    font-weight: normal;
                  }}
                  .content {{
                    padding: 30px 20px;
                    color: #333333;
                    line-height: 1.6;
                  }}
                  .button-container {{
                    text-align: center;
                    margin: 30px 0;
                  }}
                  .btn {{
                    background-color: #4a90e2;
                    color: #ffffff !important;
                    text-decoration: none;
                    padding: 14px 24px;
                    border-radius: 4px;
                    display: inline-block;
                    font-size: 16px;
                  }}
                  .footer {{
                    padding: 20px;
                    font-size: 12px;
                    color: #777777;
                    text-align: center;
                  }}
                  @media screen and (max-width: 600px) {{
                    .content {{ padding: 20px 15px; }}
                    .button-container {{ margin: 20px 0; }}
                  }}
                </style>
              </head>
              <body>
                <table width="100%" cellpadding="0" cellspacing="0">
                  <tr>
                    <td align="center">
                      <table class="email-container" cellpadding="0" cellspacing="0">
                        <!-- Header -->
                        <tr>
                          <td class="header">
                            <h1>Your Secure Login</h1>
                          </td>
                        </tr>
                        <!-- Body -->
                        <tr>
                          <td class="content">
                            <p>Hello,</p>
                            <p>Click the button below to securely log in to your account. This link will expire at <strong>{data.get('expires_at')}</strong>.</p>
                            <div class="button-container">
                              <a href="{link}" target="_blank" rel="noopener noreferrer" class="btn">Log In Now</a>
                            </div>
                            <p>If the button doesn’t work, copy and paste the following URL into your browser:</p>
                            <p><a href="{link}" target="_blank" rel="noopener noreferrer" style="color:#4a90e2; word-break:break-all;">{link}</a></p>
                            <p>If you did not request this link, you can safely ignore this email—nothing will change.</p>
                          </td>
                        </tr>
                        <!-- Footer -->
                        <tr>
                          <td class="footer">
                            &copy; 2025 Turnkey Consultancy. All rights reserved.
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
              </body>
            </html>
            """

            sent = await send_email(subject, htmlBody, recipient)

            if sent:
                # TODO: Make a db call to check process field for the magic link
                print("Sent magic link by email")

            return

    print("For some reason, link wasn't sent")


async def listener():
    # Single connection for listening to both channels
    conn = await asyncpg.connect(dsn=ASYNCPG_URL)
    await conn.add_listener(NOTIFICATION_CHANNEL, handle_notification)
    await conn.add_listener(MAGIC_LINK_CHANNEL, handle_magic_link)
    print(f"Listening on '{NOTIFICATION_CHANNEL}' and '{MAGIC_LINK_CHANNEL}'...")

    # Keep the listener alive
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(listener())
