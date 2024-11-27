import requests

class GoogleAdminClient:
    """
    A class to manage interactions with the Google Admin SDK Directory API.
    """

    def __init__(self, access_token):
        """
        Initialize the GoogleAdminManager with the given access token.

        :param access_token: OAuth 2.0 access token for Google Admin SDK.
        """
        self.base_url = "https://admin.googleapis.com/admin/directory/v1"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def fetch_all_users(self, customer="my_customer", projection="full", show_deleted=False):
        """
        Fetch all users from the Google Admin Directory API.

        :param customer: The customer ID or "my_customer" for the account.
        :param projection: The projection level, e.g., "basic" or "full".
        :param show_deleted: Whether to include deleted users. Default is False.
        :return: List of users.
        """
        url = f"{self.base_url}/users"
        params = {
            "customer": customer,
            "projection": projection
        }
        
        if show_deleted:
            params["showDeleted"] = "true"

        users = []

        response = requests.get(url, headers=self.headers, params=params)
        if response.status_code != 200:
            print(f"Failed to retrieve users. Status code: {response.status_code}")
            print(response.text)
            return users

        data = response.json()
        users.extend(data.get("users", []))

        while "nextPageToken" in data:
            params["pageToken"] = data["nextPageToken"]
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code != 200:
                print(f"Failed to retrieve users. Status code: {response.status_code}")
                print(response.text)
                return users
            data = response.json()
            users.extend(data.get("users", []))

        return users