import asyncio
import os
from dotenv import load_dotenv
load_dotenv()
import aiohttp
import json

METACULUS_TOKEN = os.getenv("METACULUS_TOKEN")

async def inspect():
    headers = {"Authorization": f"Token {METACULUS_TOKEN}"}
    async with aiohttp.ClientSession() as session:
        url = "https://www.metaculus.com/api2/questions/?forecaster_id=302314&limit=1"
        async with session.get(url, headers=headers) as resp:
            data = await resp.json()
            q = data["results"][0]
            print(json.dumps(q, indent=2))

asyncio.run(inspect())