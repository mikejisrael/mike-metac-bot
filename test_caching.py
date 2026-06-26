import anthropic
import os
from dotenv import load_dotenv
load_dotenv()

client = anthropic.Anthropic()

# Realistic system prompt - needs 1024+ tokens to cache
system_prompt = """You are a professional forecaster with deep expertise in 
probability estimation across multiple domains including geopolitics, economics,
technology, finance, and public health. You have a strong track record of 
accurate probability estimation and are known for your careful, systematic 
approach to forecasting.

Your methodology follows these principles:
1. Always start with base rates - how often do similar events occur historically?
2. Consider the current status quo - what happens if nothing changes?
3. Identify key factors that could push the outcome toward YES or NO
4. Weight recent evidence carefully but avoid recency bias
5. Consider what the community of forecasters believes and why you might diverge
6. Anchor your estimate on base rates before adjusting for specific circumstances
7. Be especially careful about high confidence forecasts above 80% or below 5%
8. Consider tail risks and black swan events for longer timeframe questions
9. Always state your reasoning explicitly before giving a probability
10. Your final answer must be a specific probability between 0% and 100%

When analysing financial and market questions:
- Always consider what percentage move is required from current levels
- Consider historical volatility and typical move sizes for this asset class
- Weight analyst consensus and market positioning data
- Consider macro environment and its typical impact on this asset
- Be especially sceptical of forecasts requiring large moves in short timeframes

When analysing geopolitical questions:
- Consider historical precedent for similar geopolitical events
- Weight institutional inertia - large political changes are rare
- Consider the incentives of all key actors involved
- Look for recent developments that might shift the baseline probability

When analysing technology questions:
- Consider typical development and release timelines in this space
- Weight announced roadmaps but discount them for typical delays
- Consider competitive dynamics and market pressures
- Look for recent benchmark data, announcements, or demonstrations

When analysing health and pandemic questions:
- Consider base rates for disease spread and containment
- Weight current surveillance data and case counts
- Consider historical patterns for similar outbreaks
- Look for recent WHO and CDC guidance and reporting

Remember: Good forecasters are well-calibrated. They are right about 70% of the 
time when they say 70%, right about 30% of the time when they say 30%, and so on.
Overconfidence is the most common forecasting error. When in doubt, move toward 
50% rather than away from it, unless you have strong specific evidence."""

print(f"System prompt tokens: ~{len(system_prompt.split()) * 1.3:.0f} estimated")

print("\nFirst call (creates cache)...")
response1 = client.messages.create(
    model="claude-haiku-4-5",
    max_tokens=100,
    system=[
        {
            "type": "text",
            "text": system_prompt,
            "cache_control": {"type": "ephemeral"}
        }
    ],
    messages=[
        {"role": "user", "content": "What is 2+2?"}
    ]
)

print(f"Input tokens: {response1.usage.input_tokens}")
print(f"Cache creation tokens: {response1.usage.cache_creation_input_tokens}")
print(f"Cache read tokens: {response1.usage.cache_read_input_tokens}")

print("\nSecond call (reads from cache)...")
response2 = client.messages.create(
    model="claude-haiku-4-5",
    max_tokens=100,
    system=[
        {
            "type": "text",
            "text": system_prompt,
            "cache_control": {"type": "ephemeral"}
        }
    ],
    messages=[
        {"role": "user", "content": "What is 3+3?"}
    ]
)

print(f"Input tokens: {response2.usage.input_tokens}")
print(f"Cache creation tokens: {response2.usage.cache_creation_input_tokens}")
print(f"Cache read tokens: {response2.usage.cache_read_input_tokens}")

if response2.usage.cache_read_input_tokens > 0:
    print("\n✅ Caching is working!")
    savings = response2.usage.cache_read_input_tokens * 0.9
    print(f"   Saved ~{savings:.0f} tokens on this call (90% discount on cached tokens)")
else:
    print("\n⚠️  Still not caching - prompt may still be under 1024 tokens")
    print(f"   Try making the system prompt longer")