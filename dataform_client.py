import requests
from google.colab import auth
from google.auth.transport.requests import Request
import google.auth


class DataformClient:
    def __init__(self, project_id, location):
        self.project_id = project_id
        self.location = location
        self.access_token = self.authenticate()

    def authenticate(self):
        """Authenticate and retrieve the access token."""
        auth.authenticate_user()
        creds, _ = google.auth.default()
        creds.refresh(Request())
        return creds.token

    def get_headers(self):
        """Generate headers for API requests."""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def get_compilation_result(self, repository_id, compilation_result_id):
        """Retrieve a compilation result."""
        url = f"https://dataform.googleapis.com/v1beta1/projects/{self.project_id}/locations/{self.location}/repositories/{repository_id}/compilationResults/{compilation_result_id}"
        response = requests.get(url, headers=self.get_headers())
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error fetching compilation result: {response.text}")

    def start_workflow(self, repository_id, compilation_result_id):
        """Start a workflow invocation."""
        workflow_url = f"https://dataform.googleapis.com/v1beta1/projects/{self.project_id}/locations/{self.location}/repositories/{repository_id}/workflowInvocations"
        body = {
            "compilationResult": f"projects/{self.project_id}/locations/{self.location}/repositories/{repository_id}/compilationResults/{compilation_result_id}"
        }
        response = requests.post(workflow_url, headers=self.get_headers(), json=body)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error starting workflow: {response.text}")

    def check_workflow_status(self, workflow_invocation_name):
        """Check the status of a workflow invocation."""
        url = f"https://dataform.googleapis.com/v1beta1/{workflow_invocation_name}"
        response = requests.get(url, headers=self.get_headers())
        if response.status_code == 200:
            status = response.json().get('state')
            return status
        else:
            raise Exception(f"Error checking workflow status: {response.text}")