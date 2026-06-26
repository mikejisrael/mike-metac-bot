import asyncio
import json
from dotenv import load_dotenv
load_dotenv()
from forecasting_tools import MetaculusClient
import time

client = MetaculusClient()

with open('batch_results.json') as f:
    results = json.load(f)

submitted = 0
for r in results.values():
    if r.get('status') == 'success' and r.get('probability'):
        try:
            client.post_binary_question_prediction(r['question_id'], r['probability'])
            print(f"✅ Q{r['question_id']}: {r['probability']:.0%} — {r.get('question_text','')[:50]}")
            submitted += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ Q{r['question_id']}: {e}")

print(f"\nDone — submitted {submitted}")