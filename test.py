import os, requests
from dotenv import load_dotenv
load_dotenv()
headers = {"Authorization": f"Token {os.getenv('METACULUS_TOKEN')}"}

for qid in [38265, 38099, 43167, 999999999]:
    r = requests.get(f"https://www.metaculus.com/api2/questions/{qid}/", headers=headers, timeout=20)
    print(qid, "->", r.status_code)
    if r.status_code == 200:
        d = r.json()
        print("   title:", d.get("title"))
    else:
        print("   body:", r.text[:150])