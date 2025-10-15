#!/usr/bin/env python3
"""
reddit_scraper.py
Scrapes submissions and top comments from a subreddit using PRAW.
Outputs JSONL (one JSON per submission) and CSV.
"""

import os
import time
import json
import re
import argparse
import logging
from typing import List, Dict, Any
from tqdm import tqdm

import praw
import pandas as pd
from dotenv import load_dotenv
from praw.models import MoreComments

# Load .env if present
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("reddit_scraper")

# Environment / credentials
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "soccercirclejerk_llm_scraper/0.1")

if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET):
    logger.error("Please set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET in env or .env")
    # don't exit immediately so script can be imported for tests; it will fail on init if used
    # but inform the user


# Utility: redact usernames / urls to avoid storing PII
USERNAME_RE = re.compile(r"(?:u/|/u/)?[A-Za-z0-9_-]{3,}")
URL_RE = re.compile(r"(https?://\S+)")
EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")


def redact_text(text: str) -> str:
    if not text:
        return ""
    # remove common 'deleted' placeholders
    text = text.replace("[deleted]", "").replace("[removed]", "")
    return text.strip()


def init_reddit_creds():
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET):
        raise RuntimeError("Missing Reddit credentials in environment.")
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )
    return reddit


def extract_top_comments(post, top_n=5) -> List[Dict[str, Any]]:
    """Return top_n top-level comments by score. Skip removed/deleted bodies."""
    try:
        post.comments.replace_more(limit=0)
    except Exception as e:
        logger.warning(f"replace_more failed for {post.id}: {e}")
    comments = []
    for c in post.comments:
        if isinstance(c, MoreComments):
            continue
        body = getattr(c, "body", None)
        if not body:
            continue
        if body.strip().lower() in ("[deleted]", "[removed]"):
            continue
        comments.append(
            {
                "id": c.id,
                "body": redact_text(body),
                "score": getattr(c, "score", 0),
                "created_utc": getattr(c, "created_utc", None),
                "is_submitter": getattr(c, "is_submitter", False),
            }
        )
    # sort by score desc
    comments = sorted(
        comments,
        key=lambda x: (x["score"] if x["score"] is not None else 0),
        reverse=True,
    )
    return comments[:top_n]


def scrape_subreddit(
    subreddit_name: str,
    sort: str = "top",
    time_filter: str = "all",
    limit: int = 250,
    top_comments: int = 5,
    output_prefix: str = "soccercirclejerk",
) -> None:
    reddit_scraper = init_reddit_creds()
    logger.info(
        f"Scraping r/{subreddit_name} | sort={sort} time_filter={time_filter} limit={limit} top_comments={top_comments}"
    )
    sub = reddit_scraper.subreddit(subreddit_name)

    submissions = []
    # Choose generator based on sort
    iteratable_results = None
    sort = sort.lower()
    if sort == "top":
        iteratable_results = sub.top(time_filter=time_filter, limit=limit)
    elif sort == "hot":
        iteratable_results = sub.hot(limit=limit)
    elif sort == "new":
        iteratable_results = sub.new(limit=limit)
    elif sort == "rising":
        iteratable_results = sub.rising(limit=limit)
    else:
        logger.warning(f"Unknown sort '{sort}', defaulting to top")
        iteratable_results = sub.top(time_filter=time_filter, limit=limit)

    out_jsonl = f"{output_prefix}_{subreddit_name}_{sort}_{time_filter}_{limit}.jsonl"
    out_csv = f"{output_prefix}_{subreddit_name}_{sort}_{time_filter}_{limit}.csv"

    jsonl_f = open(out_jsonl, "w", encoding="utf-8")
    rows = []

    count = 0
    for post in tqdm(
        iteratable_results, total=limit if isinstance(limit, int) else None
    ):
        try:
            # Basic submission fields
            post_data = {
                "id": post.id,
                "title": redact_text(post.title),
                "selftext": redact_text(getattr(post, "selftext", "")),
                "score": getattr(post, "score", None),
                "created_utc": getattr(post, "created_utc", None),
                "num_comments": getattr(post, "num_comments", None),
                # TODO: Remove following lines if truly not needed:
                # "url": "[URL]" if getattr(submission, "url", None) else None,
                # "author": "[USER]" if getattr(submission, "author", None) else None,
                "permalink": getattr(post, "permalink", None),
            }

            top_comments_list = extract_top_comments(post, top_n=top_comments)
            post_data["top_comments"] = top_comments_list

            if post_data["title"] != "":
                # write to jsonl
                jsonl_f.write(json.dumps(post_data, ensure_ascii=False) + "\n")

                # For CSV row: flatten comments into a single string separated by " ||| "
                comments_flat = " ||| ".join(
                    [
                        c["body"].replace("\n", "").replace("\r", "")
                        for c in top_comments_list
                    ]
                )
                rows.append(
                    {
                        "id": post_data["id"],
                        "title": post_data["title"],
                        "selftext": post_data["selftext"],
                        "top_comments": comments_flat,
                        "score": post_data["score"],
                        "num_comments": post_data["num_comments"],
                        "created_utc": post_data["created_utc"],
                        "permalink": post_data["permalink"],
                    }
                )

                count += 1
                # small sleep to be polite and avoid bursts; PRAW handles rate-limits but this helps????
                # time.sleep(0.1)
        except Exception as e:
            jsonl_f.close()

            logger.exception(
                f"Error processing submission id={getattr(post, 'id', None)}: {e}"
            )
            # continue to next submission

    jsonl_f.close()

    # Save CSV
    df = pd.DataFrame(rows)
    df.to_csv(out_csv, index=False)
    logger.info(f"Saved {count} submissions to {out_jsonl} and {out_csv}")


def main():
    # -h / --help text
    parser = argparse.ArgumentParser(
        description="Scrape subreddit submissions + top comments (PRAW)."
    )
    parser.add_argument(
        "--subreddit", "-s", default="soccercirclejerk", help="Subreddit name (no r/)"
    )
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        default=250,
        help="Number of submissions to fetch (max depends on Reddit & your patience)",
    )
    parser.add_argument(
        "--sort",
        default="top",
        choices=["top", "hot", "new", "rising"],
        help="Sort order",
    )
    parser.add_argument(
        "--time_filter",
        "-t",
        default="all",
        choices=["all", "day", "hour", "month", "week", "year"],
        help="Time filter for top",
    )
    parser.add_argument(
        "--top_comments",
        "-c",
        type=int,
        default=5,
        help="Top N comments to include per submission",
    )
    parser.add_argument(
        "--output_prefix", "-o", default="dataset", help="Output filename prefix"
    )
    args = parser.parse_args()

    try:
        scrape_subreddit(
            subreddit_name=args.subreddit,
            limit=args.limit,
            sort=args.sort,
            time_filter=args.time_filter,
            top_comments=args.top_comments,
            output_prefix=args.output_prefix,
        )
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        logger.info(f"{e}")


if __name__ == "__main__":
    main()
