import praw
import pandas as pd

reddit = praw.Reddit(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    user_agent="soccercirclejerk_llm"
)

subreddit = reddit.subreddit("soccercirclejerk")

data = []
for submission in subreddit.top(limit=500):  # adjust limit
    submission.comments.replace_more(limit=0)
    top_comments = [c.body for c in submission.comments[:5]]
    data.append({
        "title": submission.title,
        "selftext": submission.selftext,
        "comments": top_comments
    })

df = pd.DataFrame(data)
df.to_csv("soccercirclejerk_dataset.csv", index=False)
