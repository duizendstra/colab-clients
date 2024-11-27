# @title BigQueryManager
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import logging
import datetime

class BigQueryClient:
    """
    A class to manage interactions with Google BigQuery.
    """

    def __init__(self, project_id, credentials_path=None):
        """
        Initialize the BigQueryManager with the given project ID and optional credentials.

        :param project_id: The GCP project ID where the BigQuery dataset is located.
        :param credentials_path: Path to the service account JSON credentials file.
                                 If None, default credentials will be used.
        """
        if credentials_path:
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"Credentials file not found at {credentials_path}")
            try:
                self.client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)
            except Exception as e:
                raise ValueError(f"Failed to initialize BigQuery client with provided credentials: {e}")
        else:
            self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    def create_table(self, dataset_id, table_id, schema_json):
        """
        Create a BigQuery table using the provided JSON schema. The schema should be in a format directly compatible with BigQuery.

        :param dataset_id: The ID of the dataset where the table will be created.
        :param table_id: The ID of the table to be created.
        :param schema_json: The table schema in JSON format (list of dictionaries).
        :return: The created table object.
        """
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"

        # Create the table resource using the schema as-is
        table_resource = {
            "tableReference": {
                "projectId": self.project_id,
                "datasetId": dataset_id,
                "tableId": table_id
            },
            "schema": {
                "fields": schema_json
            }
        }

        # Create a Table object from the resource representation
        table = bigquery.Table.from_api_repr(table_resource)

        try:
            # Use the client to create the table
            table = self.client.create_table(table)
            logging.info(f"Created table {table.full_table_id}.")
            return table
        except Exception as e:
            logging.error(f"Failed to create table: {e}")
            raise

    def insert_rows_json(self, dataset_id, table_id, json_rows, add_record_load_time=True, chunk_size=500):
        """
        Insert JSON rows into a BigQuery table in chunks.

        :param dataset_id: The ID of the dataset containing the table.
        :param table_id: The ID of the table to insert data into.
        :param json_rows: A list of JSON objects to insert.
        :param add_record_load_time: Whether to add a record load timestamp to each row (default is True).
        :param chunk_size: Number of rows to insert per batch (default is 500).
        """
        # Add record load timestamp if enabled
        if add_record_load_time:
            for row in json_rows:
                row['record_load_time'] = datetime.datetime.utcnow().isoformat()

        # Create references for dataset and table
        dataset_ref = bigquery.DatasetReference(self.project_id, dataset_id)
        table_ref = bigquery.TableReference(dataset_ref, table_id)
        table = self.client.get_table(table_ref)

        # Insert rows in chunks
        for i in range(0, len(json_rows), chunk_size):
            chunk = json_rows[i:i + chunk_size]
            try:
                errors = self.client.insert_rows_json(table, chunk)
                if errors:
                    logging.error(f"Encountered errors while inserting rows (chunk {i // chunk_size + 1}): {errors}")
                    raise RuntimeError(f"Failed to insert rows for chunk {i // chunk_size + 1}: {errors}")
                else:
                    logging.info(f"Inserted {len(chunk)} rows into {table_id} (chunk {i // chunk_size + 1}).")
            except Exception as e:
                logging.error(f"Error occurred during row insertion (chunk {i // chunk_size + 1}): {e}")
                raise

    def filter_json_to_schema(self, json_obj, schema):
          """
          Filters a JSON object to match a given schema dynamically, preserving nested and repeated fields.

          :param json_obj: The input JSON object to be filtered.
          :param schema: The target schema to filter the JSON object against.
          :return: A filtered JSON object that matches the schema.
          """
          if not isinstance(json_obj, dict):
              return {}

          filtered_json = {}
          schema_fields = {field["name"]: field for field in schema}

          for field_name, field_schema in schema_fields.items():
              value = json_obj.get(field_name)

              if value is None:
                  # Skip missing fields, unless required in schema
                  if field_schema.get("mode") == "REQUIRED":
                      filtered_json[field_name] = None  # Set to None for required fields if not present
                  continue

              # Handle nested RECORD types
              if field_schema["type"] == "RECORD" and "fields" in field_schema:
                  if field_schema.get("mode") == "REPEATED":
                      # Handle repeated nested records (arrays of objects)
                      if isinstance(value, list):
                          filtered_json[field_name] = [
                              self.filter_json_to_schema(item, field_schema["fields"])
                              for item in value if isinstance(item, dict)
                          ]
                      else:
                          filtered_json[field_name] = []
                  elif isinstance(value, dict):
                      # Handle single nested record
                      filtered_json[field_name] = self.filter_json_to_schema(value, field_schema["fields"])
                  else:
                      filtered_json[field_name] = None  # Default to None if structure doesn't match

              # Handle repeated primitive fields
              elif field_schema.get("mode") == "REPEATED":
                  if isinstance(value, list):
                      filtered_json[field_name] = [
                          item for item in value if isinstance(item, (str, int, float, bool))
                      ]
                  else:
                      filtered_json[field_name] = []

              # Handle primitive fields
              else:
                  if isinstance(value, (str, int, float, bool, type(None))):  # Allow None for nullable fields
                      filtered_json[field_name] = value
                  else:
                      filtered_json[field_name] = None  # Default to None for invalid types

          return filtered_json



    def clean_and_insert_rows(self, dataset_id, table_id, json_rows, schema, add_record_load_time=True):
        """
        Cleans JSON rows based on the provided schema and inserts them into a BigQuery table.

        :param dataset_id: The ID of the dataset containing the table.
        :param table_id: The ID of the table to insert data into.
        :param json_rows: A list of JSON objects to insert.
        :param schema: The schema to use for cleaning the JSON objects.
        :param add_record_load_time: Whether to add a record load timestamp to each row (default is True).
        """
        cleaned_rows = [self.filter_json_to_schema(row, schema) for row in json_rows]
        self.insert_rows_json(dataset_id, table_id, cleaned_rows, add_record_load_time=add_record_load_time)

    def execute_query(self, query, timeout=None):
        """
        Execute a BigQuery SQL query and return the results.

        :param query: The SQL query to execute.
        :param timeout: Optional timeout value for the query execution.
        :return: A list of rows from the query result.
        """
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout <= 0 or timeout > 3600:
                raise ValueError("Timeout must be a positive number between 0 and 3600 seconds.")

        try:
            query_job = self.client.query(query, timeout=timeout)
            results = query_job.result()
            logging.info(f"Query executed successfully: {query}")
            return [dict(row) for row in results]
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            raise
