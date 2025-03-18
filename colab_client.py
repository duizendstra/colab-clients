from google.colab import auth
from google.auth import default
from google.auth.transport.requests import Request
import google.auth.iam
import google.oauth2.service_account
import logging

class ColabClient:
    """
    Manages interactions with Google APIs in Colab using service account impersonation.
    
    Args:
        service_account_email (str): The email of the service account.
        project_id (str): Google Cloud Project ID.
        scopes (List[str]): A list of OAuth2 scopes.
        subject (str, optional): The email of the user to impersonate (for domain-wide delegation).
    """

    def __init__(
        self,
        service_account_email: str,
        project_id: str,
        scopes: list[str],
        subject: str = None
    ) -> None:
        self.service_account_email = service_account_email
        self.project_id = project_id
        self.scopes = scopes
        self.subject = subject
        self.credentials = None
        self.access_token = None

    def generate_credentials(self, authenticate_in_colab: bool = True, return_token: bool = False):
        """
        Generates credentials for use in Google Colab, optionally returning the access token.

        Args:
            authenticate_in_colab (bool): Whether to call auth.authenticate_user() in Colab.
            return_token (bool, optional): Return the generated token if True.

        Returns:
            Credentials object, or (Credentials, str) if return_token is True.

        Raises:
            Exception: If an error occurs during credential generation.
        """
        try:
            auth_request = Request()
            if authenticate_in_colab:
                auth.authenticate_user()

            colab_creds, _ = default(quota_project_id=self.project_id)

            signer = google.auth.iam.Signer(
                auth_request,
                colab_creds,
                self.service_account_email
            )

            self.credentials = google.oauth2.service_account.Credentials(
                signer=signer,
                service_account_email=self.service_account_email,
                token_uri='https://oauth2.googleapis.com/token',
                scopes=self.scopes,
                subject=self.subject
            )

            self.credentials.refresh(auth_request)
            self.access_token = self.credentials.token
            logging.info("Credentials generated successfully.")

            if return_token:
                return self.credentials, self.access_token
            return self.credentials

        except Exception as e:
            logging.exception("Error generating Colab credentials.")
            raise Exception(f"An error occurred while generating Colab credentials: {e}")

    def refresh_if_needed(self):
        """
        Refresh the credentials if they are not valid or nearing expiry.
        """
        if self.credentials and not self.credentials.valid:
            logging.info("Refreshing credentials...")
            self.credentials.refresh(Request())
            self.access_token = self.credentials.token

    def get_access_token(self) -> str:
        """
        Retrieves the current access token.

        Returns:
            str: The current access token.

        Raises:
            Exception: If credentials have not been generated.
        """
        if not self.credentials:
            raise Exception("Credentials have not been generated. Call generate_credentials() first.")
        self.refresh_if_needed()
        return self.access_token
