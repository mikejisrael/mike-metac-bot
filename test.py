import glob, json
for jf in glob.glob('meta batches/batch_jobs_2*.json') + glob.glob('meta batches/batch_jobs_refresh_*.json'):
    try:
        data = json.load(open(jf))
    except Exception:
        continue
    for cid, qid in data.get('question_ids', {}).items():
        if qid == 39825:
            print(jf, '-> custom_id:', cid, 'post_id:', data.get('post_ids', {}).get(cid))