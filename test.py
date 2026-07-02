git show :2:watch_state/futureeval_seen_posts.json > seen_ours.json
git show :3:watch_state/futureeval_seen_posts.json > seen_theirs.json
git show :2:watch_state/resolution_state.json > res_ours.json
git show :3:watch_state/resolution_state.json > res_theirs.json

python -c "import json; a=json.load(open('seen_ours.json')); b=json.load(open('seen_theirs.json')); json.dump(sorted(set(a)|set(b)), open('watch_state/futureeval_seen_posts.json','w'), indent=2)"

python -c "import json; a=json.load(open('res_ours.json')); b=json.load(open('res_theirs.json')); c={**a, **b}; json.dump(c, open('watch_state/resolution_state.json','w'), indent=2)"

del seen_ours.json seen_theirs.json res_ours.json res_theirs.json