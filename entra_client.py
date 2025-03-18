# @title EntraManager
import time
import requests
from msal import ConfidentialClientApplication

class EntraClient:
    """
    A class to manage interactions with Microsoft Entra (Azure AD) through the Microsoft Graph API.
    """

    def __init__(self, tenant_id, client_id, client_secret):
        """
        Initialize the EntraManager with the given Azure AD credentials.

        :param tenant_id: Azure Active Directory Tenant ID
        :param client_id: Application (client) ID from Azure AD
        :param client_secret: Client secret from Azure AD
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.graph_api_url = "https://graph.microsoft.com/v1.0"
        self.token = None

    def _get_access_token(self):
        """
        Authenticate with Azure AD and acquire an access token.

        :return: Access token as a string
        """
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        app = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=authority,
        )
        scopes = ["https://graph.microsoft.com/.default"]
        result = app.acquire_token_for_client(scopes=scopes)

        if "access_token" in result:
            self.token = result["access_token"]
            return self.token
        else:
            raise Exception(f"Failed to acquire token: {result}")

    def fetch_all_users(self):
        """
        Fetch all users from Microsoft Entra using the Microsoft Graph API, including
        the onPremisesImmutableId and a broad set of other user fields.

        :return: List of users
        """
        if not self.token:
            self._get_access_token()

        headers = {"Authorization": f"Bearer {self.token}"}

        # Include any additional fields as needed. This is a broad (not exhaustive) set.
        select_fields = [
            "id",
            "displayName",
            "mail",
            "userPrincipalName",
            "onPremisesImmutableId",
            "onPremisesSamAccountName",
            "onPremisesSecurityIdentifier",
            "accountEnabled",
            "assignedLicenses",
            "assignedPlans",
            "businessPhones",
            "createdDateTime",
            "department",
            "companyName",
            "country",
            "city",
            "state",
            "postalCode",
            "officeLocation",
            "jobTitle",
            "mobilePhone",
            "preferredLanguage",
            "mailNickname",
            "proxyAddresses",
            "surname",
            "givenName",
            "streetAddress",
            "usageLocation",
            "externalUserState",
        ]
        
        # Construct the initial URL with the $select parameter
        url = f"{self.graph_api_url}/users?$select={','.join(select_fields)}"
        users = []

        while url:
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(
                    f"API call failed: {response.status_code}, {response.text}"
                )

            data = response.json()
            users.extend(data.get("value", []))
            # Handle pagination
            url = data.get("@odata.nextLink")

        return users

    def fetch_audit_logs(self, start_date_time=None, end_date_time=None, top=100, retry_count=5):
        """
        Fetch audit logs from Microsoft Entra (Azure AD) using the Microsoft Graph API.

        :param start_date_time: Optional start time filter in ISO8601 format (e.g., "2023-01-01T00:00:00Z").
        :param end_date_time: Optional end time filter in ISO8601 format (e.g., "2023-01-02T00:00:00Z").
        :param top: Number of logs to fetch per request (default: 100).
        :param retry_count: Number of retries for handling rate limits or transient failures (default: 5).
        :return: List of audit logs.
        """
        if not self.token:
            self._get_access_token()

        headers = {"Authorization": f"Bearer {self.token}"}
        base_url = f"{self.graph_api_url}/auditLogs/signIns"
        params = {"$top": top}

        # Add optional time filters
        if start_date_time:
            params["$filter"] = f"createdDateTime ge {start_date_time}"
        if end_date_time:
            filter_clause = params.get("$filter", "")
            if filter_clause:
                params["$filter"] = f"{filter_clause} and createdDateTime le {end_date_time}"
            else:
                params["$filter"] = f"createdDateTime le {end_date_time}"

        logs = []
        retries = 0
        url = base_url

        while url:
            print(f"Fetching logs from: {url}")

            # Use params only for the first request; subsequent requests use `@odata.nextLink`
            if url == base_url:
                response = requests.get(url, headers=headers, params=params)
            else:
                response = requests.get(url, headers=headers)

            if response.status_code == 429:  # Rate limit hit
                if retries >= retry_count:
                    print("Exceeded maximum retry attempts for rate limit.")
                    break

                retry_after = int(response.headers.get("Retry-After", 1))  # Fallback to 1 second
                wait_time = min(2 ** retries, 60)  # Exponential backoff (max 60 seconds)
                time.sleep(max(retry_after, wait_time))
                retries += 1
                continue

            if response.status_code != 200:
                print(f"Failed to retrieve logs. Status code: {response.status_code}")
                print(response.text)
                break

            data = response.json()
            logs.extend(data.get("value", []))

            # Handle pagination
            url = data.get("@odata.nextLink", None)
            retries = 0  # Reset retries on success

        return logs
