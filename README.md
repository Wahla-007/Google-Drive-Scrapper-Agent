## Google Drive → Supabase Postgres Sync

![GitHub Workflow](https://img.shields.io/badge/Python-3.13-blue?style=flat&logo=python) ![License](https://img.shields.io/badge/License-MIT-green?style=flat)

A simple and efficient Python script to **sync your Google Drive folder structure** directly into a **Supabase Postgres database**, keeping your database always in sync with Drive. It handles files, folders, and even deleted items—no duplicates, no stale data.

---

## Features

- Fully recursive sync of Google Drive folders and files.  
- Tracks file types and folder hierarchy using **Postgres ltree paths**.  
- **Upserts** existing records and deletes missing files to mirror Google Drive exactly.  
- Easy setup with a single **configuration file**—no editing the main script.  
- Compatible with Supabase Postgres and Google Drive API.  

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/drive-supabase-sync.git
cd drive-supabase-sync
2. Install Dependencies
bash
Copy code
pip install -r requirements.txt
requirements.txt includes:

psycopg2-binary (Postgres connector)

google-auth

google-auth-oauthlib

google-api-python-client

python-dotenv

3. Configure Your Environment
Create a .env file in the root of your project:

env
Copy code
DATABASE_URL=postgresql://postgres:YourSecurePassword@db.yoursupabaseproject.supabase.co:5432/postgres
ROOT_FOLDER_ID=YourGoogleDriveRootFolderID
Only one-time setup! No need to modify the main script.

4. Google API Setup
Go to the Google Cloud Console.

Create a new project and enable the Google Drive API.

Create OAuth 2.0 credentials and download credentials.json into your project folder.

5. Run the Script
bash
Copy code
python crawler.py
The script will:

Authenticate with Google Drive (first run opens a browser).

Sync all folders and files into your Supabase database.

Keep your database in sync with Drive every time you run it.

Database Schema
The nodes table structure:

Column	Type	Description
id	TEXT	Google Drive file/folder ID
name	TEXT	File or folder name
parent_id	TEXT	ID of parent folder
type	TEXT	"file" or "folder"
mime_type	TEXT	File MIME type (null for folders)
path	LTREE	Hierarchical path for querying

Tips
Use URL-encoded names for path-safe ltree storage.

Avoid editing the main script—change only .env or config.py.

You can schedule this script using cron (Linux/macOS) or Task Scheduler (Windows) for automated sync.
