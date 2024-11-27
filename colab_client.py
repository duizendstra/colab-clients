from google.colab import auth
from google.auth import default
from google.auth.transport.requests import Request
import google.auth.iam
import google.oauth2.service_account


class ColabClient:
    def __init__(self, service_account_email, project_id, scopes, subject=None):
        """
        Initializes the ColabCredentialsManager.

        Args:
        service_account_email (str): The email of the service account.
        project_id (str): Google Cloud Project ID.
        scopes (list): A list of scopes to be granted.
        subject (str, optional): The email of the user to impersonate.
        """
        self.service_account_email = service_account_email
        self.project_id = project_id
        self.scopes = scopes
        self.subject = subject
        self.credentials = None
        self.access_token = None

    def generate_credentials(self, return_token=False):
        """
        Generates credentials and an access token for use in Google Colab.

        Args:
        return_token (bool, optional): Whether to return the generated token. Defaults to False.

        Returns:
        tuple or Credentials: The credentials and the access token (if return_token is True), otherwise just the credentials.

        Raises:
        Exception: If an error occurs during the operation.
        """
        try:
            auth_request = Request()
            auth.authenticate_user()
            colab_creds, _ = default(quota_project_id=self.project_id)

            signer = google.auth.iam.Signer(
                auth_request,
                colab_creds,
                self.service_account_email
            )

            self.credentials = google.oauth2.service_account.Credentials(
                signer,
                self.service_account_email,
                token_uri='https://accounts.google.com/o/oauth2/token',
                scopes=self.scopes,
                subject=self.subject
            )

            self.credentials.refresh(auth_request)
            self.access_token = self.credentials.token

            if return_token:
                return self.credentials, self.access_token
            else:
                return self.credentials

        except Exception as e:
            raise Exception(f"An error occurred while generating Colab credentials: {e}")

    def get_access_token(self):
        """
        Retrieves the access token if it has been generated.

        Returns:
        str: The access token.

        Raises:
        Exception: If credentials have not been generated.
        """
        if not self.access_token:
            raise Exception("Access token has not been generated. Call generate_credentials() first.")
        return self.access_token
