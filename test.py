import math
from unittest.mock import MagicMock
import tournament_forecast as tf

q = MagicMock()
q.lower_bound = 300000.0
q.upper_bound = 450000.0
q.open_lower_bound = True
q.open_upper_bound = False
q.cdf_size = 201

raw = '''
10th percentile (low): 280,000
50th percentile (median): 298,000
90th percentile (high): 320,000
'''
result = tf.parse_numeric_response(raw, q)
print('Result:', result[:5] if result else None)