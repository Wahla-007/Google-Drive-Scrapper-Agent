# from __future__ import print_function
# import os
# import psycopg2
# from urllib.parse import quote_plus
# from dotenv import load_dotenv
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from google.auth.transport.requests import Request
# from googleapiclient.discovery import build

# # Load .env file (so DATABASE_URL is read automatically)
# load_dotenv()
# DATABASE_URL = os.getenv("DATABASE_URL")

# # Google Drive API scope
# SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

# def authenticate():
#     creds = None
#     if os.path.exists('token.json'):
#         creds = Credentials.from_authorized_user_file('token.json', SCOPES)
#         print("Loaded existing token.json")

#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#             print("Token refreshed")
#         else:
#             flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
#             creds = flow.run_local_server(port=0)

#         with open('token.json', 'w') as token_file:
#             token_file.write(creds.to_json())
#         print("token.json saved successfully")

#     return build('drive', 'v3', credentials=creds)

# def sync_drive(service, conn, root_folder_id):
#     """Full sync: insert/update new nodes and remove missing ones."""
#     current_nodes = {}  # {google_id: node_info}

#     def walk_folder(folder_id, parent_id=None, path=""):
#         query = f"'{folder_id}' in parents and trashed=false"
#         results = service.files().list(
#             q=query,
#             spaces='drive',
#             fields="files(id, name, mimeType)"
#         ).execute()
#         items = results.get('files', [])

#         for item in items:
#             node_type = "folder" if item['mimeType'] == 'application/vnd.google-apps.folder' else "file"
#             mime_type = None if node_type == "folder" else item['mimeType']

#             # URL-encode name for ltree-safe path
#             safe_name = quote_plus(item['name'].replace(' ', '_'))
#             node_path = f"{path}.{safe_name}" if path else safe_name

#             current_nodes[item['id']] = {
#                 'name': item['name'],
#                 'parent_id': parent_id,
#                 'type': node_type,
#                 'mime_type': mime_type,
#                 'path': node_path
#             }

#             if node_type == "folder":
#                 walk_folder(item['id'], parent_id=item['id'], path=node_path)

#     # Walk Drive
#     walk_folder(root_folder_id)

#     with conn.cursor() as cur:
#         # Upsert nodes
#         for gid, info in current_nodes.items():
#             cur.execute("""
#                 INSERT INTO nodes (id, name, parent_id, type, mime_type, path)
#                 VALUES (%s, %s, %s, %s, %s, %s)
#                 ON CONFLICT (id) DO UPDATE
#                 SET name = EXCLUDED.name,
#                     parent_id = EXCLUDED.parent_id,
#                     type = EXCLUDED.type,
#                     mime_type = EXCLUDED.mime_type,
#                     path = EXCLUDED.path;
#             """, (gid, info['name'], info['parent_id'], info['type'], info['mime_type'], info['path']))

#         # Delete nodes no longer in Drive
#         if current_nodes:
#             cur.execute("""
#                 DELETE FROM nodes
#                 WHERE id NOT IN %s
#             """, (tuple(current_nodes.keys()),))
#         else:
#             cur.execute("DELETE FROM nodes;")

#         conn.commit()

# if __name__ == '__main__':
#     service = authenticate()
#     conn = psycopg2.connect(DATABASE_URL)

#     ROOT_FOLDER_ID = "11N2Bk5SLovdPamS4cA9bq7i4pCAbjzqD"
#     print("Syncing Google Drive → Supabase Postgres...")
#     sync_drive(service, conn, ROOT_FOLDER_ID)
#     print("Done! Database now mirrors Drive exactly.")

#     conn.close()
from __future__ import print_function
import os
import psycopg2
from urllib.parse import quote_plus
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Import configuration
import config

# Google Drive API scope
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

def authenticate():
    creds = None
    if os.path.exists(config.TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(config.TOKEN_FILE, SCOPES)
        print("Loaded existing token.json")

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            print("Token refreshed")
        else:
            flow = InstalledAppFlow.from_client_secrets_file(config.CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(config.TOKEN_FILE, 'w') as token_file:
            token_file.write(creds.to_json())
        print("token.json saved successfully")

    return build('drive', 'v3', credentials=creds)

def sync_drive(service, conn, root_folder_id):
    current_nodes = {}  # {google_id: node_info}

    def walk_folder(folder_id, parent_id=None, path=""):
        query = f"'{folder_id}' in parents and trashed=false"
        results = service.files().list(
            q=query,
            spaces='drive',
            fields="files(id, name, mimeType)"
        ).execute()
        items = results.get('files', [])

        for item in items:
            node_type = "folder" if item['mimeType'] == 'application/vnd.google-apps.folder' else "file"
            mime_type = None if node_type == "folder" else item['mimeType']
            safe_name = quote_plus(item['name'].replace(' ', '_'))
            node_path = f"{path}.{safe_name}" if path else safe_name

            current_nodes[item['id']] = {
                'name': item['name'],
                'parent_id': parent_id,
                'type': node_type,
                'mime_type': mime_type,
                'path': node_path
            }

            if node_type == "folder":
                walk_folder(item['id'], parent_id=item['id'], path=node_path)

    walk_folder(root_folder_id)

    with conn.cursor() as cur:
        for gid, info in current_nodes.items():
            cur.execute("""
                INSERT INTO nodes (id, name, parent_id, type, mime_type, path)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name,
                    parent_id = EXCLUDED.parent_id,
                    type = EXCLUDED.type,
                    mime_type = EXCLUDED.mime_type,
                    path = EXCLUDED.path;
            """, (gid, info['name'], info['parent_id'], info['type'], info['mime_type'], info['path']))

        if current_nodes:
            cur.execute("DELETE FROM nodes WHERE id NOT IN %s", (tuple(current_nodes.keys()),))
        else:
            cur.execute("DELETE FROM nodes;")
        conn.commit()

if __name__ == '__main__':
    service = authenticate()
    conn = psycopg2.connect(config.DATABASE_URL)
    print("Syncing Google Drive → Supabase Postgres...")
    sync_drive(service, conn, config.ROOT_FOLDER_ID)
    print("Done! Database now mirrors Drive exactly.")
    conn.close()
