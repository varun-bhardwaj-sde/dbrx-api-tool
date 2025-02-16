import requests
import base64

class DatabricksWorkspaceAPI:
    def __init__(self, host, token):
        self.base_url = f"https://{host}/api/2.0/workspace"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def list_contents(self, path):
        url = f"{self.base_url}/list"
        params = {"path": path}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_status(self, path):
        url = f"{self.base_url}/get-status"
        params = {"path": path}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def delete(self, path, recursive=False):
        url = f"{self.base_url}/delete"
        data = {"path": path, "recursive": recursive}
        response = requests.post(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.status_code == 200

    def create_directory(self, path):
        url = f"{self.base_url}/mkdirs"
        data = {"path": path}
        response = requests.post(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.status_code == 200

    def import_notebook(self, path, language, content, format="SOURCE"):
        url = f"{self.base_url}/import"
        data = {
            "path": path,
            "language": language,
            "content": base64.b64encode(content.encode()).decode(),
            "format": format
        }
        response = requests.post(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.status_code == 200

    def export_notebook(self, path, format="SOURCE"):
        url = f"{self.base_url}/export"
        params = {"path": path, "format": format}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return base64.b64decode(response.json()["content"]).decode() if response.status_code == 200 else None

    def get_permissions(self, path):
        url = f"{self.base_url}/permissions"
        params = {"path": path}
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def update_permissions(self, path, access_control_list):
        url = f"{self.base_url}/permissions"
        data = {"access_control_list": access_control_list}
        response = requests.patch(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.json()

    def move(self, source_path, destination_path):
        url = f"{self.base_url}/move"
        data = {"source_path": source_path, "destination_path": destination_path}
        response = requests.post(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.status_code == 200
