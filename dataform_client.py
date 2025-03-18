import requests
import time
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class DataformClient:
    """
    A client for interacting with the Dataform API.  Provides methods for
    retrieving compilation results, starting workflows, checking workflow
    status, cancelling workflows, and running complete workflows with timeouts
    and incremental/full refresh options.  Accepts a pre-existing access token.
    """

    def __init__(self, project_id: str, location: str, access_token: str):
        """
        Initializes the DataformClient.

        Args:
            project_id: The Google Cloud project ID.
            location: The Dataform location (e.g., 'us-central1').
            access_token: A valid Google Cloud access token with Dataform API
                permissions.
        """
        self.project_id = project_id
        self.location = location
        self.access_token = access_token

    def get_headers(self) -> Dict[str, str]:
        """Generate headers for API requests."""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def get_compilation_result(self, repository_id: str, compilation_result_id: str) -> Dict:
        """Retrieve a compilation result."""
        url = f"https://dataform.googleapis.com/v1beta1/projects/{self.project_id}/locations/{self.location}/repositories/{repository_id}/compilationResults/{compilation_result_id}"
        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.RequestException as e:
            self._log_request_exception("Error fetching compilation result", e)
            raise  # Re-raise after logging

    def start_workflow(self, repository_id: str, compilation_result_id: str, incremental: bool = True, tags: Optional[List[str]] = None, transpile_only: bool = False) -> str:
        """Start a workflow invocation.

        Args:
            repository_id: The Dataform repository ID.
            compilation_result_id: The Dataform compilation result ID.
            incremental: (Optional) Whether to run the workflow incrementally.
                Defaults to True.  Set to False for a full refresh.
            tags: (Optional) A list of tags to filter the actions to be run.
            transpile_only: (Optional) if it is only to transpile, this is useful for dry runs.

        Returns:
            The workflow invocation name.
        """
        workflow_url = f"https://dataform.googleapis.com/v1beta1/projects/{self.project_id}/locations/{self.location}/repositories/{repository_id}/workflowInvocations"
        body = {
            "compilationResult": f"projects/{self.project_id}/locations/{self.location}/repositories/{repository_id}/compilationResults/{compilation_result_id}",
            "invocationConfig": {
              "fullyRefreshIncrementalTablesEnabled": not incremental # Dataform API uses the opposite logic
            }
        }

        if tags:
            body["invocationConfig"]["includedTags"] = tags

        if transpile_only:
            body["transpileOnly"] = transpile_only

        try:
            response = requests.post(workflow_url, headers=self.get_headers(), json=body)
            response.raise_for_status()
            workflow_invocation_name = response.json()["name"]
            return workflow_invocation_name
        except requests.exceptions.RequestException as e:
            self._log_request_exception("Error starting workflow", e)
            raise  # Re-raise after logging

    def check_workflow_status(self, workflow_invocation_name: str) -> str:
        """Check the status of a workflow invocation."""
        url = f"https://dataform.googleapis.com/v1beta1/{workflow_invocation_name}"
        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()
            status = response.json().get('state')
            return status
        except requests.exceptions.RequestException as e:
            self._log_request_exception("Error checking workflow status", e)
            raise

    def get_workflow_details(self, workflow_invocation_name: str) -> Dict:
        """Retrieve complete details of a workflow invocation."""
        url = f"https://dataform.googleapis.com/v1beta1/{workflow_invocation_name}"
        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self._log_request_exception("Error getting workflow details", e)
            raise

    def cancel_workflow(self, workflow_invocation_name: str) -> Dict:
        """Cancel a running workflow invocation."""
        url = f"https://dataform.googleapis.com/v1beta1/{workflow_invocation_name}:cancel"
        try:
            response = requests.post(url, headers=self.get_headers())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self._log_request_exception("Error cancelling workflow", e)
            raise

    def run_workflow(self, repository_id: str, compilation_result_id: str, incremental: bool = True, tags: Optional[List[str]] = None, timeout: int = 3600, poll_interval: int = 10, transpile_only: bool = False) -> Dict:
        """
        Runs a Dataform workflow and waits for its completion.

        Args:
            repository_id: The Dataform repository ID.
            compilation_result_id: The Dataform compilation result ID.
            incremental: (Optional) Whether to run the workflow incrementally.
                Defaults to True.  Set to False for a full refresh.
            tags: (Optional) A list of tags to filter the actions to be run.
            timeout: (Optional) Maximum time (in seconds) to wait for the
                workflow to complete. Defaults to 3600 (1 hour).
            poll_interval: (Optional) Time (in seconds) to wait between
                status checks. Defaults to 10 seconds.
            transpile_only: (Optional) if it is only to transpile, this is useful for dry runs.

        Returns:
            A dictionary containing the final workflow invocation details if it
            succeeds.

        Raises:
            Exception: If there are any errors fetching the compilation result,
            starting the workflow, checking the status, or if the workflow fails.
            TimeoutError: If the workflow does not complete within the timeout
            period.
        """

        # Fetch compilation result
        try:
            compilation_result = self.get_compilation_result(repository_id, compilation_result_id)
            logger.info("Compilation Result: %s", compilation_result)
        except requests.exceptions.RequestException:  # Already logged in get_compilation_result
            raise Exception(f"Error fetching compilation result.")


        # Start a workflow
        try:
            workflow_invocation_name = self.start_workflow(repository_id, compilation_result_id, incremental, tags, transpile_only)
            logger.info("Workflow started: %s", workflow_invocation_name)
        except requests.exceptions.RequestException:  # Already logged in start_workflow
           raise Exception(f"Error starting workflow.")

        if transpile_only:
            return self.get_workflow_details(workflow_invocation_name)  # Return the details directly

        # Check workflow status in a loop, with timeout
        start_time = time.time()
        while True:
            try:
                status = self.check_workflow_status(workflow_invocation_name)
                logger.info("Workflow status: %s", status)

                if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                    if status == "SUCCEEDED":
                        return self.get_workflow_details(workflow_invocation_name)
                    else:
                        raise Exception(f"Workflow failed with status: {status}. Details: {self.get_workflow_details(workflow_invocation_name)}")

                elapsed_time = time.time() - start_time
                if elapsed_time > timeout:
                    raise TimeoutError(f"Workflow timed out after {timeout} seconds.")

                time.sleep(poll_interval)

            except TimeoutError:  # Catch timeout error and re-raise
                raise
            except requests.exceptions.RequestException: # Already logged in check_workflow_status
                raise Exception("Error checking workflow status.")

    def _log_request_exception(self, message: str, e: requests.exceptions.RequestException):
        """Helper function for consistent logging of request exceptions."""
        if isinstance(e, requests.exceptions.ConnectionError):
            logger.error("%s: Connection error: %s", message, e)
        elif isinstance(e, requests.exceptions.Timeout):
            logger.error("%s: Request timed out: %s", message, e)
        elif isinstance(e, requests.exceptions.HTTPError):
            logger.error("%s: HTTP error (%s): %s", message, e.response.status_code, e)
        else:
            logger.error("%s: Request error: %s", message, e)