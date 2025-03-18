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

    def update_user(self, user_key, user_body):
        """
        Update a user in the Google Admin Directory, such as custom schemas or any other fields.

        :param user_key: The user's primary email address, alias email, or unique user ID.
        :param user_body: A dictionary containing the user fields to be updated.
                          For example, updating custom schemas might look like:
                          {
                            "customSchemas": {
                              "MyCustomSchema": {
                                "customField1": "Value1",
                                "customField2": "Value2"
                              }
                            }
                          }
        :return: The updated user resource as a dictionary, or None if failed.
        """
        url = f"{self.base_url}/users/{user_key}"
        # PATCH allows partial updates (vs. PUT which requires the entire user object)
        response = requests.patch(url, headers=self.headers, json=user_body)

        if response.status_code == 200:
            print(f"User {user_key} updated successfully.")
            return response.json()
        else:
            print(f"Failed to update user {user_key}. Status code: {response.status_code}")
            print(response.text)
            return None
