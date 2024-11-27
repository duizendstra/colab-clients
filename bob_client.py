import requests

class BobClient:
    """
    A class to manage interactions with the HiBob API.
    """

    def __init__(self, api_key):
        """
        Initialize the HiBobManager with the given API key.

        :param api_key: Your HiBob API key
        """
        self.base_url = "https://api.hibob.com/v1"
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Basic {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def fetch_all_employees(self, show_inactive=False):
        """
        Fetch all employees from HiBob, with optional handling for inactive employees.

        :param show_inactive: Boolean indicating whether to include inactive employees.
        :return: List of employees
        """
        url = f"{self.base_url}/people/search"
        payload = {
            "showInactive": show_inactive,
            "humanReadable": "REPLACE",
            "fields": [
              "root.id",
              "root.email",
              "root.fullName",
              "root.displayName",
              "root.creationDateTime",
              "root.companyId",
              "work.siteId",
              "work"
            ]
        }
        employees = []

        response = requests.post(url, json=payload, headers=self.headers)
        if response.status_code != 200:
            print(f"Failed to retrieve employees. Status code: {response.status_code}")
            print(response.text)
            return employees

        data = response.json()
        employees.extend(data.get("employees", []))

        return employees