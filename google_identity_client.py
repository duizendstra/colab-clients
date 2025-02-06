import requests
from typing import Dict, Optional
from requests.exceptions import RequestException, HTTPError

class GoogleIdentityClientError(Exception):
    """Base class for exceptions in this module."""

class HttpError(GoogleIdentityClientError):
    """Exception raised for HTTP errors."""
    def __init__(self, status_code: int, message: str):
        super().__init__(f'HTTP error: {status_code} - {message}')
        self.status_code = status_code

class RequestError(GoogleIdentityClientError):
    """Exception raised for request errors."""
    def __init__(self, message: str):
        super().__init__(f'Request error: {message}')

class UserInvitationClient:
    """
    A class to manage interactions with the Google Cloud Identity User Invitations API (v1beta1).
    """

    def __init__(self, access_token: str):
        """
        Initialize the UserInvitationClient with the given access token.

        :param access_token: OAuth 2.0 access token for Google Cloud Identity.
        """
        if not access_token:
            raise ValueError("Access token cannot be empty.")

        self.base_url = "https://cloudidentity.googleapis.com/v1beta1"
        self.access_token = access_token

    def _call_api(self, method: str, endpoint: str, params: Optional[Dict] = None, json: Optional[Dict] = None) -> Dict:
        """
        Internal helper function to make API calls.

        :param method: HTTP method (GET, POST, PATCH, etc.).
        :param endpoint: API endpoint (e.g., "/customers/userinvitations").
        :param params: Query parameters.
        :param json: JSON payload for the request body.
        :return: JSON response as a dictionary.
        :raises HttpError: For HTTP errors.
        :raises RequestError: For other request errors.
        """
        url = f"{self.base_url}/{endpoint}"  # Corrected URL construction
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        try:
            response = requests.request(method, url, headers=headers, params=params, json=json, timeout=60)
            response.raise_for_status()
            return response.json() if response.content else {}
        except HTTPError as http_error:
            raise HttpError(http_error.response.status_code, str(http_error)) from http_error
        except RequestException as e:
            raise RequestError(str(e)) from e


    def is_invitable_user(self, customer: str, user_email: str) -> Dict:
        """
        Check if a user is invitable using the customers.userinvitations.isInvitableUser API.

        :param customer:  The customer ID (e.g., "customers/C01234abc").
        :param user_email: The email address of the user to check.
        :return: The API response as a dictionary.  Returns an empty dict on error.
        :raises: HttpError, RequestError
        """
        if not customer or not user_email:
            raise ValueError("Customer and user_email cannot be empty.")

        endpoint = f"{customer}/userinvitations:isInvitableUser"
        params = {"userEmail": user_email}
        return self._call_api("GET", endpoint, params=params)