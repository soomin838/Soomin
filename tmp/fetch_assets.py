
import requests
import os
from pathlib import Path

def fetch_image(prompt, filename):
    url = f"https://image.pollinations.ai/prompt/{prompt}?width=1024&height=1024&nologo=true&model=flux"
    print(f"Fetching: {filename}...")
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            target = Path(f"storage/ui/mascot/{filename}")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(response.content)
            print(f"Saved: {target}")
        else:
            print(f"Failed: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

# High-quality 3D Glassy Jelly Mascot Prompts
prompts = {
    "idle.png": "3D render of a cute round jelly robot named Rezy, translucent glass material with soft internal glow, pastel pink and sky blue colors, large cute eyes, gentle smile, floating mid-air, soft studio lighting, high depth of field, pure white background, masterpiece, kitsch premium style",
    "running.png": "3D render of Rezy the glass slime robot in an action pose, dynamic motion blur, glowing sparkles around, translucent jelly material, vibrant colors, pure white background, ultra-detailed, premium tech mascot",
    "success.png": "3D render of Rezy the glass robot celebrating, heart eyes, floating stars and confetti, translucent gummy texture, internal radiant light, pure white background, cute and kitsch",
    "error.png": "3D render of Rezy the glass robot looking dizzy and sad, bandage on head, soft red internal glow, translucent material, melting slightly, pure white background, cute chibi style",
    "warning.png": "3D render of Rezy the glass robot looking curious and cautious, magnifying glass, yellow internal glow, translucent textures, pure white background, premium detail"
}

for name, p in prompts.items():
    fetch_image(p.replace(" ", "%20"), name)
