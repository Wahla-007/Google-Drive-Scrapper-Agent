# Google Drive → Supabase Postgres Sync

*A complete Python solution to mirror your Google Drive folder structure into a Supabase Postgres database.*

## Overview

This project provides a Python script that:

* Connects to your **Google Drive** using OAuth 2.0
* Recursively scans all **folders and files**
* Inserts, updates, or deletes records in a **Supabase Postgres database**
* Keeps your database in sync with Drive

**Why use this?**

* Track files, folders, and their hierarchy
* Maintain a reliable copy of Drive metadata
* Enable analytics, search, or integrations on your Drive content

## Features

* **Recursive traversal** of Google Drive
* **File and folder tracking** with types and MIME types
* **Database mirroring**: deleted files are removed automatically
* **ltree paths** to track folder hierarchy for efficient queries
* **Single configuration file** for easy setup

## Technology Stack

| Component        | Purpose                              |
| ---------------- | ------------------------------------ |
| Python 3.13      | Core language                        |
| psycopg2         | PostgreSQL connector                 |
| Supabase         | Hosted Postgres database             |
| Google Drive API | Metadata access                      |
| python-dotenv    | Environment variable management      |
| ltree            | PostgreSQL hierarchical path storage |

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/drive-supabase-sync.git
cd drive-supabase-sync
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

Dependencies include:

* `psycopg2-binary`
* `google-auth`
* `google-auth-oauthlib`
* `google-api-python-client`
* `python-dotenv`

### 3. Configure Environment

Create a `.env` file in the project root:

```env
DATABASE_URL=postgresql://postgres:YourSecurePassword@db.yoursupabaseproject.supabase.co:5432/postgres
ROOT_FOLDER_ID=YourGoogleDriveRootFolderID
```

> The `.env` file stores all dynamic values, so no need to modify the main Python script.

### 4. Google Drive API Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project
3. Enable **Google Drive API**
4. Create **OAuth 2.0 Client Credentials**
5. Download `credentials.json` to the project folder

### 5. Run the Script

```bash
python crawler.py
```

First run:

* Opens a browser for Google authentication
* Saves the token in `token.json`
* Syncs your Drive to Supabase

Subsequent runs use `token.json` automatically.

## Database Schema

Table: `nodes`

| Column    | Type  | Description                       |
| --------- | ----- | --------------------------------- |
| id        | TEXT  | Google Drive file/folder ID       |
| name      | TEXT  | File or folder name               |
| parent_id | TEXT  | Parent folder ID                  |
| type      | TEXT  | "file" or "folder"                |
| mime_type | TEXT  | File MIME type (null for folders) |
| path      | LTREE | Hierarchical path for queries     |

## How it Works

1. **Authentication** – Script authenticates with Google Drive via OAuth 2.0
2. **Folder Traversal** – Recursively fetches all files/folders
3. **Database Upsert** – Inserts new items, updates existing ones
4. **Deletion Sync** – Removes nodes from DB that no longer exist in Drive
5. **Path Handling** – Uses `ltree` paths for hierarchy queries

## Example Workflow

```
Drive:
  Root
    ├─ Folder A
    │   ├─ File1.pdf
    │   └─ File2.docx
    └─ Folder B
        └─ Subfolder1
            └─ File3.txt
```

Database `nodes` table after sync:

| id     | name       | parent_id | type   | mime_type                                                               | path                          |
| ------ | ---------- | --------- | ------ | ----------------------------------------------------------------------- | ----------------------------- |
| abc123 | Folder A   | ROOT_ID   | folder | NULL                                                                    | Folder_A                      |
| def456 | File1.pdf  | abc123    | file   | application/pdf                                                         | Folder_A.File1.pdf            |
| ghi789 | File2.docx | abc123    | file   | application/vnd.openxmlformats-officedocument.wordprocessingml.document | Folder_A.File2.docx           |
| jkl012 | Folder B   | ROOT_ID   | folder | NULL                                                                    | Folder_B                      |
| mno345 | Subfolder1 | jkl012    | folder | NULL                                                                    | Folder_B.Subfolder1           |
| pqr678 | File3.txt  | mno345    | file   | text/plain                                                              | Folder_B.Subfolder1.File3.txt |

## Best Practices

* Only change `.env` for setup
* Avoid editing main script
* Schedule recurring sync with **cron** or **Windows Task Scheduler**
* Keep `token.json` secure

## License

MIT License – feel free to use, modify, and share.

## Author

– [GitHub](https://github.com/Wahla-007)
