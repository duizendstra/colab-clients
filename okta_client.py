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
        last_next_link = None  # Keep track of the last 'next' link to detect duplicates

        while url:
            print(f"Fetching logs from: {url}")
            response = requests.get(url, headers=self.headers, params=params)

            if response.status_code == 429:
                if retry_attempts >= max_retries:
                    print("Exceeded maximum retry attempts for rate limit.")
                    break

                retry_after = int(response.headers.get("Retry-After", 1))  # Fallback to 1 second
                backoff_time = min(2 ** retry_attempts, 60)  # Exponential backoff (capped at 60 seconds)
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
            if not data:  # Break if no data is returned
                print("No more logs available.")
                break

            logs.extend(data)

            # Reset retry attempts on success
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
                if next_link == last_next_link:  # Detect if 'next' link is stuck
                    print("Pagination is stuck. Exiting loop.")
                    break
                last_next_link = next_link  # Update the last 'next' link
                url = next_link
                params = None  # Parameters are included in the next link
            else:
                url = None  # Exit loop when no 'next' link is found



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
                params = None  # Parameters are included in the next_link
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
                params = None  # Parameters are included in the next_link
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

