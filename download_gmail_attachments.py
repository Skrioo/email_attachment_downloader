import os
import shutil
import base64
import datetime
import logging
import sqlite3
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email import message_from_bytes
from dateutil import parser
import time

# Gmail API scope
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Paths for attachments on OneDrive and temp directory in WSL
BASE_DIR = "/mnt/c/Users/ismar/OneDrive/ISO doo/Script"
TEMP_DIR = "/mnt/f/BOG JE PROGRAM/GMAIL API/Temp_Attachments"
DB_PATH = 'attachments.db'

# Configure logging
logging.basicConfig(
    filename='attachment_download.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Create table for attachments if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS downloaded_attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL,
            attachment_id TEXT NOT NULL,
            file_path TEXT NOT NULL,
            download_date TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

# Make sure temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

# Authenticate and create Gmail API service with token caching
def authenticate_gmail():
    creds = None
    token_file = 'token.json'

    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request()) # type: ignore
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_file, 'w') as token:
            token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds)
    return service

# Check if attachment already downloaded
def is_attachment_downloaded(message_id, attachment_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id FROM downloaded_attachments
        WHERE message_id = ? AND attachment_id = ?
    ''', (message_id, attachment_id))
    result = cursor.fetchone()
    conn.close()
    return result is not None

# Log downloaded attachment in database
def log_attachment_download(message_id, attachment_id, file_path):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO downloaded_attachments (message_id, attachment_id, file_path, download_date)
        VALUES (?, ?, ?, ?)
    ''', (message_id, attachment_id, file_path, datetime.datetime.now().isoformat()))
    conn.commit()
    conn.close()

# Search for emails with specified labels
def search_emails(service, user_id='me', label_ids=[]):
    try:
        results = service.users().messages().list(userId=user_id, labelIds=label_ids).execute()
        messages = results.get('messages', [])
        return messages
    except HttpError as error:
        logging.error(f'An error occurred during email search: {error}')
        return []

# Download and save attachments in organized folders by month and year
def download_attachments(service, messages):
    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id']).execute()
        headers = msg.get("payload", {}).get("headers", [])

        # Extract and parse the date
        date_header = next(header for header in headers if header["name"] == "Date")
        email_date = parser.parse(date_header["value"])

        # Set folder structure based on year and month (with month number for ordering)
        folder_path = os.path.join(BASE_DIR, str(email_date.year), email_date.strftime('%m-%B'))
        os.makedirs(folder_path, exist_ok=True)

        for part in msg.get('payload', {}).get('parts', []):
            if part['filename']:
                attachment_id = part['body'].get('attachmentId')
                message_id = message['id']

                # Check database to avoid duplicate downloads
                if is_attachment_downloaded(message_id, attachment_id):
                    logging.info(f"Skipped already downloaded attachment: {part['filename']}")
                    continue

                # Download the attachment to the temporary directory
                attachment = service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                
                # Save file in TEMP_DIR first
                temp_file_path = os.path.join(TEMP_DIR, part['filename'])
                with open(temp_file_path, 'wb') as f:
                    f.write(data)
                
                logging.info(f"Downloaded attachment to temp directory: {temp_file_path}")
                
                # Move file to the final OneDrive location with retries
                final_file_path = os.path.join(folder_path, part['filename'])
                for attempt in range(3):  # Retry up to 3 times
                    try:
                        shutil.move(temp_file_path, final_file_path)
                        logging.info(f"Moved attachment to OneDrive folder: {final_file_path}")
                        
                        # Log attachment in the database
                        log_attachment_download(message_id, attachment_id, final_file_path)
                        break  # Exit retry loop if successful
                    except Exception as e:
                        logging.warning(f"Attempt {attempt + 1} to move file failed: {e}")
                        time.sleep(2)  # Wait a bit before retrying
                else:
                    logging.error(f"Failed to move attachment after 3 attempts: {temp_file_path}")

# Main function to authenticate, initialize DB, search emails, and download attachments
def main():
    # Initialize the database
    init_db()

    # Authenticate and create Gmail API service
    service = authenticate_gmail()

    # Specify labels for email search
    label_ids = ['Label_8001345254298395342']  # Replace with actual label IDs
    messages = search_emails(service, label_ids=label_ids)
    
    if not messages:
        logging.info("No messages found.")
        return
    
    # Download attachments
    download_attachments(service, messages)

if __name__ == '__main__':
    main()