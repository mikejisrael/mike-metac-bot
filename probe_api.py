import asyncio
import os
from dotenv import load_dotenv
load_dotenv()
import aiohttp
import json

METACULUS_TOKEN = os.getenv("METACULUS_TOKEN")

async def probe():
    headers = {"Authorization": f"Token {METACULUS_TOKEN}"}
    async with aiohttp.ClientSession() as session:
        # Q38063 - One Big Beautiful Bill - we know this has community forecasters
        url = "https://www.metaculus.com/api2/questions/?ids=38063&limit=1"
        async with session.get(url, headers=headers) as resp:
            data = await resp.json()
            r = data["results"][0]
            q = r.get("question", {})
            print("=== AGGREGATIONS ===")
            print(json.dumps(q.get("aggregations"), indent=2))
            print("\n=== TOP LEVEL KEYS ===")
            print(list(r.keys()))
            print("\n=== QUESTION KEYS ===")
            print(list(q.keys()))
            print("\n=== nr_forecasters / forecasts_count ===")
            print(f"nr_forecasters: {r.get('nr_forecasters')}")
            print(f"forecasts_count: {r.get('forecasts_count')}")

asyncio.run(probe())