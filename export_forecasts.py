import asyncio
import csv
import json
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

def export_forecasts():
    # Load forecasts
    if not os.path.exists("batch_results.json"):
        print("❌ batch_results.json not found")
        return
    
    if not os.path.exists("batch_jobs.json"):
        print("❌ batch_jobs.json not found")
        return
    
    with open("batch_results.json") as f:
        results = json.load(f)
    
    with open("batch_jobs.json") as f:
        job_info = json.load(f)
    
    community_preds = job_info.get("community_predictions", {})
    resolve_times = job_info.get("resolve_times", {})
    categories = job_info.get("categories", {})
    
    print(f"Loaded {len(results)} forecasts")
    print(f"Community predictions available: {sum(1 for v in community_preds.values() if v is not None)}/{len(community_preds)}")
    
    rows = []
    for custom_id, item in results.items():
        if item.get("status") != "success" or item.get("probability") is None:
            continue
        
        my_prob = item["probability"]
        community = community_preds.get(custom_id)
        gap = round((my_prob - community) * 100, 1) if community is not None else None
        cats = categories.get(custom_id, [])
        
        rows.append({
            "question_id":        item["question_id"],
            "question":           item.get("question_text", "")[:120],
            "my_forecast_%":      round(my_prob * 100, 1),
            "community_median_%": round(community * 100, 1) if community is not None else "",
            "gap_points":         gap,
            "category":           cats[0] if cats else "",
            "resolve_time":       (resolve_times.get(custom_id) or "")[:10],
            "url":                f"https://www.metaculus.com/questions/{item['question_id']}/"
        })
    
    if not rows:
        print("No rows generated.")
        return
    
    rows.sort(key=lambda x: abs(x["gap_points"]) if x["gap_points"] is not None else 0, reverse=True)
    
    filename = f"metaculus_forecasts_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    
    with_community = [r for r in rows if r["community_median_%"] != ""]
    print(f"✅ Exported {len(rows)} forecasts to {filename}")
    print(f"   {len(with_community)}/{len(rows)} have community prediction data")
    
    if with_community:
        print(f"\nTop 10 biggest gaps (your forecast vs community):")
        print(f"{'Question':<55} {'Yours':>6} {'Comm':>6} {'Gap':>7} {'Cat':<12}")
        print("-" * 95)
        for r in rows[:10]:
            cm = f"{r['community_median_%']}%" if r['community_median_%'] != "" else "n/a"
            gp = f"{r['gap_points']}pt" if r['gap_points'] is not None else "n/a"
            print(f"{str(r['question'])[:55]:<55} {str(r['my_forecast_%'])+'%':>6} {cm:>6} {gp:>7} {r['category']:<12}")
    else:
        print("\nNo community data in current batch_jobs.json — will populate on next batch run.")
        print("Your forecasts:")
        for r in rows[:10]:
            print(f"  {r['my_forecast_%']}% — {r['question'][:70]}")

if __name__ == "__main__":
    export_forecasts()