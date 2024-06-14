import praw
import pandas as pd
import time
import logging
import configparser

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read configuration from a config file
config = configparser.ConfigParser()
config.read('config.ini')

# Const
OUTPUT_PATH = config['PATHS']['OutputPath']
FILE_PATH = config['PATHS']['FilePath']
CLIENT_ID = config['REDDIT']['ClientId']
CLIENT_SECRET = config['REDDIT']['ClientSecret']
USER_AGENT = 'webscrapping'

# Initialize Reddit client
reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)

def extract_comments(file_path):
    """Extract comments from Reddit based on submissions in a given CSV file."""
    try:
        data = pd.read_csv(file_path)
        logging.info(f"{len(data)} submissions loaded from {file_path}")
        
        for index, row in data.iterrows():
            logging.info(f"Processing submission {index + 1}/{len(data)}: {row['submission_id']}")
            process_submission(row)
    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")

def process_submission(submission):
    """Process a single Reddit submission extracting comments and saving them to a CSV."""
    comment_list = []
    submission_obj = reddit.submission(submission['submission_id'])
    submission_obj.comments.replace_more(limit=0) # this makes sure every comment is read

    for comment in submission_obj.comments.list():
        comment_data = [
            submission['subr'], submission['submission_tit'], submission['submission_id'],
            submission['subm_selftext'], comment.body, comment.author, comment.id, 
            comment.parent_id, comment.created_utc, submission['language'], submission['en_event_title']
        ]
        comment_list.append(comment_data)
    
    save_comments(comment_list, submission)

def save_comments(comments, submission):
    """Save extracted comments to a CSV file."""
    df = pd.DataFrame(comments, columns=[
        'subreddit', 'subm_title', 'subm_id', 'subm_body', 'comment_body', 'author_name', 
        'comment_id', 'comment_parrent', 'created_utc', 'subr_lang', 'en_event_title'
    ])
    file_name = f"{submission['language']}_{submission['en_event_title']}_{submission['submission_id']}.csv"
    file_path = f"{OUTPUT_PATH}/{file_name}"
    df.to_csv(file_path)
    logging.info(f"Results written to {file_path}")

if __name__ == "__main__":
    extract_comments(FILE_PATH)
