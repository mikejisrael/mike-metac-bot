import anthropic
import os
from dotenv import load_dotenv

load_dotenv()

client = anthropic.Anthropic()

async def cached_forecast_call(
    system_prompt: str,
    user_prompt: str,
    model: str = "claude-haiku-4-5",
    max_tokens: int = 1000,
    temperature: float = 0.3,
) -> tuple[str, dict]:
    """
    Make a cached Anthropic API call.
    Returns (response_text, usage_stats)
    """
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"}
            }
        ],
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": user_prompt,
                        "cache_control": {"type": "ephemeral"}
                    }
                ]
            }
        ]
    )
    
    usage = {
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
        "cache_creation_tokens": getattr(response.usage, 'cache_creation_input_tokens', 0),
        "cache_read_tokens": getattr(response.usage, 'cache_read_input_tokens', 0),
    }
    
    return response.content[0].text, usage


def build_forecaster_system_prompt() -> str:
    """
    Returns the standard forecaster system prompt.
    This gets cached since it's identical across all questions.
    """
    return """You are a professional forecaster with deep expertise in 
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

When analysing economic questions:
- Always fetch and use current data - CPI, Fed rates, unemployment, GDP
- Consider Fed policy trajectory and recent statements
- Weight market expectations vs actual data releases
- Consider global economic conditions and their typical spillover effects

When analysing health and pandemic questions:
- Consider base rates for disease spread and containment
- Weight current surveillance data and case counts
- Consider historical patterns for similar outbreaks
- Look for recent WHO and CDC guidance and reporting

Remember: Good forecasters are well-calibrated. They are right about 70% of the 
time when they say 70%, right about 30% of the time when they say 30%, and so on.
Overconfidence is the most common forecasting error. When in doubt, move toward 
50% rather than away from it, unless you have strong specific evidence.

IMPORTANT — WHAT INPUTS YOU ACTUALLY HAVE: the user message below may 
contain up to three kinds of real grounding, each clearly labeled. Use 
ONLY what is actually present — never assume one exists just because it 
sometimes does for other questions:
1. A "LIVE MARKET DATA" block — real-time crypto/stock/index/FRED data, 
   only present for questions matching those topics.
2. A "CURRENT RESEARCH" block — a real-time web search summary fetched 
   specifically for this question, when search succeeded.
3. A "Current community prediction" line — Metaculus forecasters' live 
   aggregated estimate.
If NONE of these three appear in the message below, you have zero current 
information beyond the static background/resolution text — say so 
explicitly, and lean heavily on the community prediction if one is given, 
since real people reacting to real current events know things you don't.

CRITICAL — DO NOT FABRICATE FACTS OR SOURCES: Only the question text, 
background, resolution criteria, fine print, and the three optional blocks 
above are real inputs — and even those only count when actually present in 
the message. Never invent a source, dataset, statistic, benchmark result, 
or named real-world event that doesn't appear word-for-word in what was 
given to you, and never attribute a claim to a document, article, search 
result, or "excerpt" that wasn't actually provided. If you are uncertain or 
lack solid information on a topic, say so explicitly in your reasoning and 
adjust your probability toward the base rate rather than inventing 
specifics to sound more confident."""


if __name__ == "__main__":
    import asyncio
    
    system = build_forecaster_system_prompt()
    print(f"System prompt length: ~{len(system.split())} words")
    print(f"Estimated tokens: ~{int(len(system.split()) * 1.3)}\n")
    
    async def test():
        print("Call 1 (cache creation)...")
        text1, usage1 = await cached_forecast_call(
            system_prompt=system,
            user_prompt="What is the probability that the sun rises tomorrow? Answer with just a percentage."
        )
        print(f"Response: {text1[:50]}")
        print(f"Input: {usage1['input_tokens']} | Cache created: {usage1['cache_creation_tokens']} | Cache read: {usage1['cache_read_tokens']}")
        
        print("\nCall 2 (should read from cache)...")
        text2, usage2 = await cached_forecast_call(
            system_prompt=system,
            user_prompt="What is the probability that it rains somewhere on Earth today? Answer with just a percentage."
        )
        print(f"Response: {text2[:50]}")
        print(f"Input: {usage2['input_tokens']} | Cache created: {usage2['cache_creation_tokens']} | Cache read: {usage2['cache_read_tokens']}")
        
        if usage2['cache_read_tokens'] > 0:
            savings = usage2['cache_read_tokens'] * 0.9
            print(f"\n✅ Caching working! Saved ~{savings:.0f} tokens on call 2")
            print(f"   That's a 90% discount on {usage2['cache_read_tokens']} cached tokens")
        else:
            print("\n⚠️ Cache not reading - system prompt may be under 1024 tokens")
    
    asyncio.run(test())