"""
llm_layer/web_search.py
Optional hook to fetch a brief market summary for a symbol before
generating a trade card. Currently a stub — returns empty string.
 
Why this exists:
  The local llama3.2:3b model has no internet access, so live web
  context isn't available in the initial implementation. This slot
  is reserved so that when the agent moves to the DGX Spark with a
  larger model, real market summaries can be plugged in here without
  restructuring the trade_card.py pipeline.
 
Future implementation options:
  - Tavily / Perplexity API for a one-paragraph market summary
  - RSS scraper for recent headlines on the underlying
  - FRED API for macro context (VIX, rates, SPX trend)
  - Mistral:7b on DGX Spark with web tool access
 
Usage (in trade_card.py or retrieval.py):
  from llm_layer.web_search import fetch_market_summary
  summary = fetch_market_summary("IWM")
  # Returns "" until a real implementation is plugged in
"""
from __future__ import annotations
 
import logging
 
logger = logging.getLogger(__name__)
 
 
def fetch_market_summary(symbol: str) -> str:
    """
    Fetches a brief market summary for the given symbol.
 
    Currently a stub — returns empty string.
    Replace the body of this function when web access is available.
 
    Args:
        symbol: Ticker symbol e.g. "IWM", "NVDA"
 
    Returns:
        A short plain-text market summary, or "" if unavailable.
    """
    logger.debug(f"web_search: stub called for {symbol} — returning empty string")
    return ""