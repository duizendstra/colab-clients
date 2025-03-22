import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
import urllib.parse
import json

class GoogleOAuthTokenFetcherInteractive:
    """
    Fetches OAuth 2.0 tokens for Google APIs in Colab interactively,
    storing the token locally for persistence and refresh.
    """

    def __init__(self, client_id, client_secret,
                 scopes=None, redirect_uri='https://localhost:8080',
                 token_file='token.json'):
        """
        Initializes the TokenFetcher.

        Args:
            client_id (str): Your application's client ID.
            client_secret (str): Your application's client secret.
            scopes (list): List of OAuth 2.0 scopes.
            redirect_uri (str): The redirect URI.
            token_file (str): Path to store the token (defaults to 'token.json').
        """
        if scopes is None:
            scopes = []
        self.client_config = {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uris": [redirect_uri],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://accounts.google.com/o/oauth2/token"
            }
        }
        self.scopes = scopes
        self.redirect_uri = redirect_uri
        self.token_file = token_file
        self.credentials = None

    def fetch_token(self):
        """
        Fetches or refreshes the OAuth 2.0 token. Handles manual
        code copy, and stores/loads the token from the specified file.
        """
        creds = None
        if os.path.exists(self.token_file):
            try:
                creds = Credentials.from_authorized_user_file(self.token_file, self.scopes)
            except Exception as e:
                print(f"Error loading existing token: {e}")
                os.remove(self.token_file)  # Remove corrupted token file

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    print(f"Error refreshing token: {e}")
                    if os.path.exists(self.token_file):
                         os.remove(self.token_file)
                    creds = None
            if not creds: # Do a full auth flow
                flow = Flow.from_client_config(
                    self.client_config,
                    scopes=self.scopes,
                    redirect_uri=self.redirect_uri
                )
                auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')

                print('Please visit this URL to authorize the application:')
                print(auth_url)
                print("\nAfter authorizing, copy the ENTIRE URL from the address bar of the redirected page.")
                print("Paste the ENTIRE URL below:")

                redirect_response = input("Enter the ENTIRE redirected URL: ")

                try:
                    parsed_url = urllib.parse.urlparse(redirect_response)
                    query_params = urllib.parse.parse_qs(parsed_url.query)
                    code = query_params['code'][0]
                except Exception as e:
                    print(f"Error parsing URL: {e}. Make sure you copied the *entire* URL.")
                    return None

                try:
                    flow.fetch_token(code=code)
                    creds = flow.credentials
                    with open(self.token_file, 'w') as token:
                        token.write(creds.to_json())
                    print("Authorization successful and token saved!")
                except Exception as e:
                    print(f"An error occurred during token fetching: {e}")
                    return None

        self.credentials = creds
        return creds


    def get_credentials(self):
      """Returns credentials, if they exists"""
      return self.credentials