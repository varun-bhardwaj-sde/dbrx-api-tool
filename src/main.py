{\rtf1\ansi\ansicpg1252\cocoartf2761
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 import requests\
import base64\
\
class DatabricksWorkspaceAPI:\
    def __init__(self, host, token):\
        self.base_url = f"https://\{host\}/api/2.0/workspace"\
        self.headers = \{\
            "Authorization": f"Bearer \{token\}",\
            "Content-Type": "application/json"\
        \}\
\
    def list_contents(self, path):\
        url = f"\{self.base_url\}/list"\
        data = \{"path": path\}\
        response = requests.get(url, headers=self.headers, json=data)\
        return response.json()\
\
    def get_status(self, path):\
        url = f"\{self.base_url\}/get-status"\
        data = \{"path": path\}\
        response = requests.get(url, headers=self.headers, json=data)\
        return response.json()\
\
    def delete(self, path, recursive=False):\
        url = f"\{self.base_url\}/delete"\
        data = \{"path": path, "recursive": recursive\}\
        response = requests.post(url, headers=self.headers, json=data)\
        return response.status_code == 200\
\
    def create_directory(self, path):\
        url = f"\{self.base_url\}/mkdirs"\
        data = \{"path": path\}\
        response = requests.post(url, headers=self.headers, json=data)\
        return response.status_code == 200\
\
    def import_notebook(self, path, language, content, format="SOURCE"):\
        url = f"\{self.base_url\}/import"\
        data = \{\
            "path": path,\
            "language": language,\
            "content": base64.b64encode(content.encode()).decode(),\
            "format": format\
        \}\
        response = requests.post(url, headers=self.headers, json=data)\
        return response.status_code == 200\
\
    def export_notebook(self, path, format="SOURCE"):\
        url = f"\{self.base_url\}/export"\
        data = \{"path": path, "format": format\}\
        response = requests.get(url, headers=self.headers, json=data)\
        if response.status_code == 200:\
            return base64.b64decode(response.json()["content"]).decode()\
        return None\
\
    def get_permissions(self, path):\
        url = f"\{self.base_url\}/permissions"\
        params = \{"path": path\}\
        response = requests.get(url, headers=self.headers, params=params)\
        return response.json()\
\
    def update_permissions(self, path, access_control_list):\
        url = f"\{self.base_url\}/permissions"\
        data = \{"path": path, "access_control_list": access_control_list\}\
        response = requests.patch(url, headers=self.headers, json=data)\
        return response.json()\
\
    def move(self, source_path, destination_path):\
        url = f"\{self.base_url\}/move"\
        data = \{"source_path": source_path, "destination_path": destination_path\}\
        response = requests.post(url, headers=self.headers, json=data)\
        return response.status_code == 200\
}