from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Gmail API scope for accessing Gmail labels
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# Authenticate and create a Gmail API service
def authenticate_gmail():
    creds = None
    # Authenticate using OAuth 2.0
    flow = InstalledAppFlow.from_client_secrets_file(
        'credentials.json', SCOPES)
    creds = flow.run_local_server(port=0)
    service = build('gmail', 'v1', credentials=creds)
    return service

# List all labels in Gmail with their names and IDs
def list_labels(service):
    try:
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        
        if not labels:
            print("No labels found.")
            return

        print("Labels:")
        for label in labels:
            print(f"Name: {label['name']}, ID: {label['id']}")
    
    except HttpError as error:
        print(f'An error occurred: {error}')

# Main function to authenticate and list labels
def main():
    # Authenticate and create service
    service = authenticate_gmail()
    
    # List all labels with their names and IDs
    list_labels(service)

if __name__ == '__main__':
    main()

