import requests
import time

class OktaClient:
    """
    A class to manage interactions with the Okta API.
    """

    def __init__(self, okta_domain, api_token):
        """
        Initialize the OktaManager with the given domain and API token.

        :param okta_domain: Your Okta domain (e.g., 'yourcompany' if your domain is 'yourcompany.okta.com')
        :param api_token: Your Okta API token with appropriate permissions
        """
        self.base_url = f"https://{okta_domain}.okta.com/api/v1"
        self.api_token = api_token
        self.headers = {
            "Authorization": f"SSWS {self.api_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def fetch_system_logs(self, since=None, until=None, limit=1000, max_retries=5):
        """
        Fetch system logs from Okta with pagination handling and exponential backoff.

        :param since: (Optional) ISO8601 timestamp to filter logs since this time.
        :param until: (Optional) ISO8601 timestamp to filter logs until this time.
        :param limit: Maximum number of logs per request (default: 1000).
        :param max_retries: Maximum number of retries for 429 errors.
        :return: List of system logs
        """
        url = f"{self.base_url}/logs"
        params = {
            "limit": limit
        }
        if since:
            params["since"] = since
        if until:
            params["until"] = until

        logs = []
        retry_attempts = 0
        last_next_link = None

        while url:
            print(f"Fetching logs from: {url}")
            response = requests.get(url, headers=self.headers, params=params)

            if response.status_code == 429:
                if retry_attempts >= max_retries:
                    print("Exceeded maximum retry attempts for rate limit.")
                    break
                retry_after = int(response.headers.get("Retry-After", 1))
                backoff_time = min(2 ** retry_attempts, 60)
                wait_time = max(retry_after, backoff_time)
                print(f"Rate limit hit. Retrying after {wait_time} seconds...")
                time.sleep(wait_time)
                retry_attempts += 1
                continue

            if response.status_code != 200:
                print(f"Failed to retrieve logs. Status code: {response.status_code}")
                print(response.text)
                break

            data = response.json()
            if not data:
                print("No more logs available.")
                break

            logs.extend(data)

            retry_attempts = 0

            # Parse the Link header for pagination
            next_link = None
            links = response.headers.get('Link')
            if links:
                link_headers = requests.utils.parse_header_links(links.strip(' <>'))
                for link in link_headers:
                    if link.get('rel') == 'next':
                        next_link = link.get('url')
                        break

            if next_link:
                if next_link == last_next_link:
                    print("Pagination is stuck. Exiting loop.")
                    break
                last_next_link = next_link
                url = next_link
                params = None
            else:
                url = None

        return logs

    def fetch_deactivated_users(self):
        """
        Fetch all deactivated users (status = DEPROVISIONED) from Okta with pagination handling.

        :return: List of deactivated users
        """
        url = f"{self.base_url}/users"
        params = {
            "filter": 'status eq "DEPROVISIONED"',
            "limit": "200"
        }
        users = []

        while url:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code != 200:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                print(response.text)
                break

            data = response.json()
            users.extend(data)

            # Parse the Link header for pagination
            next_link = None
            links = response.headers.get('Link')
            if links:
                link_headers = requests.utils.parse_header_links(links.strip(' <>'))
                for link in link_headers:
                    if link.get('rel') == 'next':
                        next_link = link.get('url')
                        break

            if next_link:
                url = next_link
                params = None
            else:
                url = None

        return users

    def fetch_users(self, filter_query=None):
        """
        Fetch users from Okta with pagination handling and optional filter.

        :param filter_query: Optional filter query to apply when fetching users.
        :return: List of users
        """
        url = f"{self.base_url}/users"
        params = {
            "limit": "200"
        }
        if filter_query:
            params["filter"] = filter_query

        users = []

        while url:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code != 200:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                print(response.text)
                break

            data = response.json()
            users.extend(data)

            # Parse the Link header for pagination
            next_link = None
            links = response.headers.get('Link')
            if links:
                link_headers = requests.utils.parse_header_links(links.strip(' <>'))
                for link in link_headers:
                    if link.get('rel') == 'next':
                        next_link = link.get('url')
                        break

            if next_link:
                url = next_link
                params = None
            else:
                url = None

        return users

    def fetch_all_workflows(self):
        """
        Fetch all workflows from Okta.

        :return: List of workflows
        """
        url = f"{self.base_url}/workflows"
        workflows = []

        while url:
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                print(f"Failed to retrieve workflows. Status code: {response.status_code}")
                print(response.text)
                break

            data = response.json()
            workflows.extend(data)

            # Parse the Link header for pagination
            next_link = None
            links = response.headers.get('Link')
            if links:
                link_headers = requests.utils.parse_header_links(links.strip(' <>'))
                for link in link_headers:
                    if link.get('rel') == 'next':
                        next_link = link.get('url')
                        break

            if next_link:
                url = next_link
            else:
                url = None

        return workflows

    def fetch_app_users(self, app_id, limit=200):
        """
        Fetch all users assigned to a given app by ID, using pagination.

        :param app_id: The Okta app's ID
        :param limit: Number of users to fetch per page (default: 200)
        :return: List of assigned users
        """
        url = f"{self.base_url}/apps/{app_id}/users"
        params = {"limit": limit}
        assigned_users = []

        while url:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code != 200:
                print(f"Failed to retrieve assigned users for App ID: {app_id}. Status code: {response.status_code}")
                print(response.text)
                break

            data = response.json()
            assigned_users.extend(data)

            # Parse the Link header for pagination
            next_link = None
            links = response.headers.get('Link')
            if links:
                link_headers = requests.utils.parse_header_links(links.strip(' <>'))
                for link in link_headers:
                    if link.get('rel') == 'next':
                        next_link = link.get('url')
                        break

            if next_link:
                url = next_link
                # For subsequent pages, params are embedded in the link
                params = None
            else:
                url = None

        return assigned_users
