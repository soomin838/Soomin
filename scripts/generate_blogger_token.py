from __future__ import annotations

from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

ROOT = Path(__file__).resolve().parent.parent
CLIENT_SECRET = ROOT / "config" / "client_secrets.json"
TOKEN_OUT = ROOT / "config" / "blogger_token.json"


def main() -> None:
    flow = InstalledAppFlow.from_client_secrets_file(
        str(CLIENT_SECRET),
        scopes=[
            "https://www.googleapis.com/auth/blogger",
        ],
    )
    creds = flow.run_local_server(port=0)
    TOKEN_OUT.write_text(creds.to_json(), encoding="utf-8")
    print(f"Saved token to {TOKEN_OUT}")


if __name__ == "__main__":
    main()
