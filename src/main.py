import requests
import base64
import json
from typing import Dict, List, Optional, Union, Any

class DatabricksAPIError(Exception):
    """Exception raised for errors in the Databricks API."""
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Databricks API Error (Status {status_code}): {message}")

class DatabricksWorkspaceAPI:
    def __init__(self, host, token):
        """Initialize the Databricks Workspace API client.
        
        Args:
            host: The Databricks host (e.g., 'adb-123456789.1.azuredatabricks.net')
            token: The Databricks personal access token
        """
        self.base_url = f"https://{host}/api/2.0/workspace"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def _handle_response(self, response):
        """Handle API response and raise appropriate exceptions."""
        try:
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            error_msg = "Unknown error"
            try:
                error_data = response.json()
                if "error_code" in error_data and "message" in error_data:
                    error_msg = f"{error_data['error_code']}: {error_data['message']}"
                elif "message" in error_data:
                    error_msg = error_data["message"]
            except:
                error_msg = response.text or str(e)
            
            raise DatabricksAPIError(response.status_code, error_msg)
    
    def list_contents(self, path: str) -> Dict[str, Any]:
        """List the contents of a directory.
        
        Args:
            path: The path to list contents from
            
        Returns:
            Dictionary containing objects and directories
        """
        url = f"{self.base_url}/list"
        params = {"path": path}
        response = requests.get(url, headers=self.headers, params=params)
        self._handle_response(response)
        return response.json()

    def get_status(self, path: str) -> Dict[str, Any]:
        """Get the status of a workspace object.
        
        Args:
            path: The path to get status for
            
        Returns:
            Dictionary containing object metadata
        """
        url = f"{self.base_url}/get-status"
        params = {"path": path}
        response = requests.get(url, headers=self.headers, params=params)
        self._handle_response(response)
        return response.json()

    def delete(self, path: str, recursive: bool = False) -> bool:
        """Delete a workspace object.
        
        Args:
            path: The path to delete
            recursive: If True, delete recursively for directories
            
        Returns:
            True if deletion was successful
        """
        url = f"{self.base_url}/delete"
        data = {"path": path, "recursive": recursive}
        response = requests.post(url, headers=self.headers, json=data)
        self._handle_response(response)
        return response.status_code == 200

    def create_directory(self, path: str) -> bool:
        """Create a directory.
        
        Args:
            path: The path to create
            
        Returns:
            True if directory creation was successful
        """
        url = f"{self.base_url}/mkdirs"
        data = {"path": path}
        response = requests.post(url, headers=self.headers, json=data)
        self._handle_response(response)
        return response.status_code == 200

    def import_notebook(self, path: str, language: str, content: str, format: str = "SOURCE", overwrite: bool = False) -> bool:
        """Import a notebook into the workspace.
        
        Args:
            path: The path to import to
            language: The language of the notebook (PYTHON, SCALA, SQL, R)
            content: The content of the notebook
            format: The format of the content (SOURCE, HTML, JUPYTER, DBC)
            overwrite: If True, overwrite existing notebook
            
        Returns:
            True if import was successful
        """
        url = f"{self.base_url}/import"
        data = {
            "path": path,
            "language": language,
            "content": base64.b64encode(content.encode()).decode(),
            "format": format,
            "overwrite": overwrite
        }
        response = requests.post(url, headers=self.headers, json=data)
        self._handle_response(response)
        return response.status_code == 200

    def export_notebook(self, path: str, format: str = "SOURCE") -> Optional[str]:
        """Export a notebook from the workspace.
        
        Args:
            path: The path to export from
            format: The format to export as (SOURCE, HTML, JUPYTER, DBC)
            
        Returns:
            The content of the notebook or None if export failed
        """
        url = f"{self.base_url}/export"
        params = {"path": path, "format": format}
        response = requests.get(url, headers=self.headers, params=params)
        self._handle_response(response)
        return base64.b64decode(response.json()["content"]).decode() if response.status_code == 200 else None

    def get_permissions(self, path: str) -> Dict[str, Any]:
        """Get permissions for a workspace object.
        
        Args:
            path: The path to get permissions for
            
        Returns:
            Dictionary containing permission information
        """
        url = f"{self.base_url}/permissions"
        params = {"path": path}
        response = requests.get(url, headers=self.headers, params=params)
        self._handle_response(response)
        return response.json()

    def update_permissions(self, path: str, access_control_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Update permissions for a workspace object.
        
        Args:
            path: The path to update permissions for
            access_control_list: List of access control items
            
        Returns:
            Dictionary containing updated permission information
        """
        url = f"{self.base_url}/permissions"
        data = {"access_control_list": access_control_list}
        response = requests.patch(url, headers=self.headers, json=data)
        self._handle_response(response)
        return response.json()

    def move(self, source_path: str, destination_path: str, overwrite: bool = False) -> bool:
        """Move a workspace object.
        
        Args:
            source_path: The source path to move from
            destination_path: The destination path to move to
            overwrite: If True, overwrite destination if it exists
            
        Returns:
            True if move was successful
        """
        url = f"{self.base_url}/move"
        data = {
            "source_path": source_path, 
            "destination_path": destination_path,
            "overwrite": overwrite
        }
        response = requests.post(url, headers=self.headers, json=data)
        self._handle_response(response)
        return response.status_code == 200
        
    def copy(self, source_path: str, destination_path: str, overwrite: bool = False) -> bool:
        """Copy a workspace object.
        
        Args:
            source_path: The source path to copy from
            destination_path: The destination path to copy to
            overwrite: If True, overwrite destination if it exists
            
        Returns:
            True if copy was successful
        """
        # First check if source exists
        try:
            self.get_status(source_path)
        except DatabricksAPIError as e:
            raise DatabricksAPIError(e.status_code, f"Source path does not exist: {source_path}")
            
        # Check if destination exists and we're not overwriting
        if not overwrite:
            try:
                self.get_status(destination_path)
                raise DatabricksAPIError(409, f"Destination already exists: {destination_path}")
            except DatabricksAPIError as e:
                if e.status_code != 404:  # 404 is good - means destination doesn't exist
                    raise
        
        # If source is a notebook, export and import it
        try:
            source_info = self.get_status(source_path)
            if source_info.get("object_type") == "NOTEBOOK":
                content = self.export_notebook(source_path)
                return self.import_notebook(
                    destination_path, 
                    source_info.get("language", "PYTHON"),
                    content,
                    overwrite=overwrite
                )
            elif source_info.get("object_type") == "DIRECTORY":
                # Create destination directory
                self.create_directory(destination_path)
                
                # List contents of source directory
                contents = self.list_contents(source_path)
                
                # Copy each item recursively
                success = True
                for item in contents.get("objects", []):
                    item_path = item.get("path")
                    item_name = item_path.split("/")[-1]
                    new_dest = f"{destination_path}/{item_name}"
                    if not self.copy(item_path, new_dest, overwrite):
                        success = False
                        
                return success
            else:
                raise DatabricksAPIError(400, f"Unsupported object type: {source_info.get('object_type')}")
        except Exception as e:
            if isinstance(e, DatabricksAPIError):
                raise
            raise DatabricksAPIError(500, f"Error during copy operation: {str(e)}")
            
    def search(self, path: str, recursive: bool = True, file_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Search for objects in the workspace.
        
        Args:
            path: The path to search in
            recursive: If True, search recursively
            file_types: List of file types to filter by (NOTEBOOK, DIRECTORY, LIBRARY, etc.)
            
        Returns:
            List of objects matching the search criteria
        """
        results = []
        file_types = file_types or ["NOTEBOOK", "DIRECTORY", "LIBRARY", "REPO"]
        
        try:
            contents = self.list_contents(path)
            
            for item in contents.get("objects", []):
                item_type = item.get("object_type")
                
                if item_type in file_types:
                    results.append(item)
                
                if recursive and item_type == "DIRECTORY":
                    item_path = item.get("path")
                    sub_results = self.search(item_path, recursive, file_types)
                    results.extend(sub_results)
                    
            return results
        except Exception as e:
            if isinstance(e, DatabricksAPIError):
                raise
            raise DatabricksAPIError(500, f"Error during search operation: {str(e)}")
            
    def exists(self, path: str) -> bool:
        """Check if a path exists in the workspace.
        
        Args:
            path: The path to check
            
        Returns:
            True if the path exists, False otherwise
        """
        try:
            self.get_status(path)
            return True
        except DatabricksAPIError as e:
            if e.status_code == 404:
                return False
            raise
        import requests
import base64
import json
from typing import Dict, List, Optional, Union, Any

class DatabricksAPIError(Exception):
    """Exception raised for errors in the Databricks API."""
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Databricks API Error (Status {status_code}): {message}")

class DatabricksWorkspaceAPI:
    def __init__(self, host, token):
        """Initialize the Databricks Workspace API client.
        
        Args:
            host: The Databricks host (e.g., 'adb-123456789.1.azuredatabricks.net')
            token: The Databricks personal access token
        """
        self.base_url = f"https://{host}/api/2.0/workspace"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def _handle_response(self, response):
        """Handle API response and raise appropriate exceptions."""
        try:
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            error_msg = "Unknown error"
            try:
                error_data = response.json()
                if "error_code" in error_data and "message" in error_data:
                    error_msg = f"{error_data['error_code']}: {error_data['message']}"
                elif "message" in error_data:
                    error_msg = error_data["message"]
            except:
                error_msg = response.text or str(e)
            
            raise DatabricksAPIError(response.status_code, error_msg)
    
    def list_contents(self, path: str) -> Dict[str, Any]:
        """List the contents of a directory.
        
        Args:
            path: The path to list contents from
            
        Returns:
            Dictionary containing objects and directories
        """
        url = f"{self.base_url}/list"
        params = {"path": path}
        response = requests.get(url, headers=self.headers, params=params)
        self._handle_response(response)
        return response.json()

    def get_status(self, path: str) -> Dict[str, Any]:
        """Get the status of a workspace object.
        
        Args:
            path: The path to get status for
            
        Returns:
            Dictionary containing object metadata
        """
        url = f"{self.base_url}/get-status"
        params = {"path": path}
        response = requests.get(url, headers=self.headers, params=params)
        self._handle_response(response)
        return response.json()

    def delete(self, path: str, recursive: bool = False) -> bool:
        """Delete a workspace object.
        
        Args:
            path: The path to delete
            recursive: If True, delete recursively for directories
            
        Returns:
            True if deletion was successful
        """
        url = f"{self.base_url}/delete"
        data = {"path": path, "recursive": recursive}
        response = requests.post(url, headers=self.headers, json=data)
        self._handle_response(response)
        return response.status_code == 200

    def create_directory(self, path: str) -> bool:
        """Create a directory.
        
        Args:
            path: The path to create
            
        Returns:
            True if directory creation was successful
        """
        url = f"{self.base_url}/mkdirs"
        data = {"path": path}
        response = requests.post(url, headers=self.headers, json=data)
        self._handle_response(response)
        return response.status_code == 200

    def import_notebook(self, path: str, language: str, content: str, format: str = "SOURCE", overwrite: bool = False) -> bool:
        """Import a notebook into the workspace.
        
        Args:
            path: The path to import to
            language: The language of the notebook (PYTHON, SCALA, SQL, R)
            content: The content of the notebook
            format: The format of the content (SOURCE, HTML, JUPYTER, DBC)
            overwrite: If True, overwrite existing notebook
            
        Returns:
            True if import was successful
        """
        url = f"{self.base_url}/import"
        data = {
            "path": path,
            "language": language,
            "content": base64.b64encode(content.encode()).decode(),
            "format": format,
            "overwrite": overwrite
        }
        response = requests.post(url, headers=self.headers, json=data)
        self._handle_response(response)
        return response.status_code == 200

    def export_notebook(self, path: str, format: str = "SOURCE") -> Optional[str]:
        """Export a notebook from the workspace.
        
        Args:
            path: The path to export from
            format: The format to export as (SOURCE, HTML, JUPYTER, DBC)
            
        Returns:
            The content of the notebook or None if export failed
        """
        url = f"{self.base_url}/export"
        params = {"path": path, "format": format}
        response = requests.get(url, headers=self.headers, params=params)
        self._handle_response(response)
        return base64.b64decode(response.json()["content"]).decode() if response.status_code == 200 else None

    def get_permissions(self, path: str) -> Dict[str, Any]:
        """Get permissions for a workspace object.
        
        Args:
            path: The path to get permissions for
            
        Returns:
            Dictionary containing permission information
        """
        url = f"{self.base_url}/permissions"
        params = {"path": path}
        response = requests.get(url, headers=self.headers, params=params)
        self._handle_response(response)
        return response.json()

    def update_permissions(self, path: str, access_control_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Update permissions for a workspace object.
        
        Args:
            path: The path to update permissions for
            access_control_list: List of access control items
            
        Returns:
            Dictionary containing updated permission information
        """
        url = f"{self.base_url}/permissions"
        data = {"access_control_list": access_control_list}
        response = requests.patch(url, headers=self.headers, json=data)
        self._handle_response(response)
        return response.json()

    def move(self, source_path: str, destination_path: str, overwrite: bool = False) -> bool:
        """Move a workspace object.
        
        Args:
            source_path: The source path to move from
            destination_path: The destination path to move to
            overwrite: If True, overwrite destination if it exists
            
        Returns:
            True if move was successful
        """
        url = f"{self.base_url}/move"
        data = {
            "source_path": source_path, 
            "destination_path": destination_path,
            "overwrite": overwrite
        }
        response = requests.post(url, headers=self.headers, json=data)
        self._handle_response(response)
        return response.status_code == 200
        
    def copy(self, source_path: str, destination_path: str, overwrite: bool = False) -> bool:
        """Copy a workspace object.
        
        Args:
            source_path: The source path to copy from
            destination_path: The destination path to copy to
            overwrite: If True, overwrite destination if it exists
            
        Returns:
            True if copy was successful
        """
        # First check if source exists
        try:
            self.get_status(source_path)
        except DatabricksAPIError as e:
            raise DatabricksAPIError(e.status_code, f"Source path does not exist: {source_path}")
            
        # Check if destination exists and we're not overwriting
        if not overwrite:
            try:
                self.get_status(destination_path)
                raise DatabricksAPIError(409, f"Destination already exists: {destination_path}")
            except DatabricksAPIError as e:
                if e.status_code != 404:  # 404 is good - means destination doesn't exist
                    raise
        
        # If source is a notebook, export and import it
        try:
            source_info = self.get_status(source_path)
            if source_info.get("object_type") == "NOTEBOOK":
                content = self.export_notebook(source_path)
                return self.import_notebook(
                    destination_path, 
                    source_info.get("language", "PYTHON"),
                    content,
                    overwrite=overwrite
                )
            elif source_info.get("object_type") == "DIRECTORY":
                # Create destination directory
                self.create_directory(destination_path)
                
                # List contents of source directory
                contents = self.list_contents(source_path)
                
                # Copy each item recursively
                success = True
                for item in contents.get("objects", []):
                    item_path = item.get("path")
                    item_name = item_path.split("/")[-1]
                    new_dest = f"{destination_path}/{item_name}"
                    if not self.copy(item_path, new_dest, overwrite):
                        success = False
                        
                return success
            else:
                raise DatabricksAPIError(400, f"Unsupported object type: {source_info.get('object_type')}")
        except Exception as e:
            if isinstance(e, DatabricksAPIError):
                raise
            raise DatabricksAPIError(500, f"Error during copy operation: {str(e)}")
            
    def search(self, path: str, recursive: bool = True, file_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Search for objects in the workspace.
        
        Args:
            path: The path to search in
            recursive: If True, search recursively
            file_types: List of file types to filter by (NOTEBOOK, DIRECTORY, LIBRARY, etc.)
            
        Returns:
            List of objects matching the search criteria
        """
        results = []
        file_types = file_types or ["NOTEBOOK", "DIRECTORY", "LIBRARY", "REPO"]
        
        try:
            contents = self.list_contents(path)
            
            for item in contents.get("objects", []):
                item_type = item.get("object_type")
                
                if item_type in file_types:
                    results.append(item)
                
                if recursive and item_type == "DIRECTORY":
                    item_path = item.get("path")
                    sub_results = self.search(item_path, recursive, file_types)
                    results.extend(sub_results)
                    
            return results
        except Exception as e:
            if isinstance(e, DatabricksAPIError):
                raise
            raise DatabricksAPIError(500, f"Error during search operation: {str(e)}")
            
    def exists(self, path: str) -> bool:
        """Check if a path exists in the workspace.
        
        Args:
            path: The path to check
            
        Returns:
            True if the path exists, False otherwise
        """
        try:
            self.get_status(path)
            return True
        except DatabricksAPIError as e:
            if e.status_code == 404:
                return False
            raise
        

        
