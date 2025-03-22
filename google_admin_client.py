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


from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import Optional, List, Dict, Any
from google.oauth2.credentials import Credentials

class AdminSDKClient:
    """
    Client for interacting with the Google Admin SDK Reports API.

    Provides methods for fetching customer usage reports, handling pagination
    and errors gracefully.
    """

    def __init__(self, credentials: Credentials, customer_id: str = "my_customer") -> None:
        """
        Initializes the AdminSDKClient.

        Args:
            credentials:  The Google API credentials (obtained via OAuth 2.0).
            customer_id: The customer ID. Use 'my_customer' for your own account.
                         Defaults to "my_customer".
        """
        self.credentials: Credentials = credentials
        self.customer_id: str = customer_id
        self.service = build("admin", "reports_v1", credentials=self.credentials)


    def get_usage_report(self, date: str, parameters: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Fetches customer usage data for a given date.

        Args:
            date: The date for which to fetch usage data (YYYY-MM-DD).
            parameters: (Optional) Comma-separated string of parameters to retrieve.
                         See: https://developers.google.com/admin-sdk/reports/v1/reference/usage-ref-appendix-a/customer-usage

        Returns:
            A list of dictionaries containing the usage records, or None if an error occurred.
            Handles pagination automatically.
        """

        all_usage_records: List[Dict[str, Any]] = []
        next_page_token: Optional[str] = None

        try:
            while True:
                results = (
                    self.service.customerUsageReports()
                    .get(date=date, customerId=self.customer_id, pageToken=next_page_token, parameters=parameters)
                    .execute()
                )
                if "usageReports" in results:
                    all_usage_records.extend(results["usageReports"])
                next_page_token = results.get("nextPageToken")
                if not next_page_token:
                    break

        except HttpError as error:
            print(f"An error occurred while fetching usage data: {error}")
            return None

        return all_usage_records

    def get_user_usage_report(self, user_key: str, date:str, parameters: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Fetches user usage reports.

        Args:
            user_key: The user key, can be the primary email, alias email, or unique user ID.
            date: The date (YYYY-MM-DD) for the report.
            parameters: (Optional) comma-separated string of usage parameters.

        Returns:
            A list of usage records, or None on error.
        """
        all_usage_records = []
        next_page_token = None

        try:
            while True:
                results = (
                    self.service.userUsageReport()
                    .get(userKey=user_key, date=date, pageToken=next_page_token, parameters=parameters)
                    .execute()
                )

                if "usageReports" in results:
                    all_usage_records.extend(results["usageReports"])
                next_page_token = results.get("nextPageToken")

                if not next_page_token:
                    break
        except HttpError as error:
            print(f"An error occurred while fetching user usage data: {error}")
            return None

        return all_usage_records

    def get_activity_events(self, application_name: str, actor_email: Optional[str] = None,
                            event_name: Optional[str] = None, start_time: Optional[str] = None,
                            end_time: Optional[str] = None, parameters: Optional[str] = None
                            ) -> Optional[List[Dict[str, Any]]]:
        """Fetches activity events for a given application.

        Args:
            application_name: The name of the application to fetch events for.
                See: https://developers.google.com/admin-sdk/reports/v1/reference/activities/list
            actor_email: (Optional) Email address of the actor.
            event_name: (Optional) Name of the event being queried.
            start_time: (Optional) Start time of the query in RFC3339 format (YYYY-MM-DDTHH:MM:SSZ).
            end_time: (Optional) End time of the query in RFC3339 format (YYYY-MM-DDTHH:MM:SSZ).
            parameters: (Optional) Comma-separated list of event parameters to retrieve.

        Returns:
            A list of activity events, or None on error.
        """
        all_activities = []
        next_page_token = None

        try:
            while True:
                results = (
                    self.service.activities()
                    .list(
                        customerId=self.customer_id,
                        applicationName=application_name,
                        actorEmail=actor_email,
                        eventName=event_name,
                        startTime=start_time,
                        endTime=end_time,
                        pageToken=next_page_token,
                        parameters=parameters,
                    )
                    .execute()
                )

                if "items" in results:
                    all_activities.extend(results["items"])
                next_page_token = results.get("nextPageToken")

                if not next_page_token:
                    break

        except HttpError as error:
            print(f"An error occurred while fetching activity events: {error}")
            return None
        return all_activities