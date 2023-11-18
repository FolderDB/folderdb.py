import os
import json
import base64
import shutil

class FileDatabase:
    def __init__(self, base_path):
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)

    def create_table(self, table_name, columns):
        table_path = os.path.join(self.base_path, table_name)
        os.makedirs(table_path, exist_ok=True)

        # Store table schema inside the table directory
        with open(os.path.join(table_path, "schema.json"), "w") as schema_file:
            json.dump(columns, schema_file)

    def insert_row(self, table_name, row_data):
        table_path = os.path.join(self.base_path, table_name)

        row_number = len(os.listdir(table_path)) + 1
        row_folder = os.path.join(table_path, f"{row_number}.row")
        os.makedirs(row_folder)

        # Ensure row_data is a string, if it's a list, convert it to a string
        if isinstance(row_data, list):
            row_data = ",".join(str(element) for element in row_data)  # Convert list elements to strings

        # Encode and store row data
        encoded_data = base64.b64encode(row_data.encode() if isinstance(row_data, str) else row_data)
        with open(os.path.join(row_folder, "row_content"), "wb") as row_file:
            row_file.write(encoded_data)

    def retrieve_rows(self, table_name):
        table_path = os.path.join(self.base_path, table_name)
        rows = []
        for folder in os.listdir(table_path):
            row_folder = os.path.join(table_path, folder)
            if os.path.isdir(row_folder):
                with open(os.path.join(row_folder, "row_content"), "rb") as row_file:
                    encoded_data = row_file.read()
                    decoded_data = base64.b64decode(encoded_data).decode()
                    rows.append(decoded_data)
        return rows

    def delete_table(self, table_name):
        table_path = os.path.join(self.base_path, table_name)
        if os.path.exists(table_path):
            shutil.rmtree(table_path)  # Remove the table directory and its contents
        else:
            raise FileNotFoundError(f"Table '{table_name}' does not exist.")

    def delete_row(self, table_name, row_number):
        row_folder = os.path.join(self.base_path, table_name, f"{row_number}.row")
        if os.path.exists(row_folder):
            shutil.rmtree(row_folder)  # Remove the row directory and its contents
        else:
            raise FileNotFoundError(f"Row '{row_number}' in table '{table_name}' does not exist.")

    def delete_database(self):
        if os.path.exists(self.base_path):
            shutil.rmtree(self.base_path)  # Remove the entire database directory and its contents
        else:
            raise FileNotFoundError("Database does not exist.")

    def select_rows(self, table_name, condition_func):
        table_path = os.path.join(self.base_path, table_name)
        rows = []
        for folder in os.listdir(table_path):
            row_folder = os.path.join(table_path, folder)
            if os.path.isdir(row_folder):
                with open(os.path.join(row_folder, "row_content"), "rb") as row_file:
                    encoded_data = row_file.read()
                    decoded_data = base64.b64decode(encoded_data).decode()
                    if condition_func(decoded_data):
                        rows.append(decoded_data)
        return rows
