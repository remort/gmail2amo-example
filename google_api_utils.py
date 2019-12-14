import os
import pickle
from typing import Optional, Dict, Any

from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource

# If modifying these scopes, delete the file token.pickle.
# https://developers.google.com/gmail/api/auth/scopes
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
USER_ID = '***@***.ru'

flow = InstalledAppFlow.from_client_secrets_file(
    'credentials.json', SCOPES)


def get_service() -> Resource:
    creds: Optional[Credentials] = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # TODO: обрабатывать кейс отсутсвия реквизитов для авторизации путем показа url для авторизации
            flow: InstalledAppFlow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('gmail', 'v1', credentials=creds, cache_discovery=False)


def get_labels(service):
    """Получает лэйблы ящика созданные пользователем, не системные."""
    labels_request: Dict[str, Any] = service.users().labels().list(userId=USER_ID).execute()
    return {label['name']: label['id'] for label in labels_request['labels']}
