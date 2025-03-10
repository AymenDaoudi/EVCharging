from datetime import datetime
import os
import lakefs
from lakefs.client import Client

REPOSITORY = os.getenv("LAKEFS_REPOSITORY", "charging-data")

LAKEFS_HOST = os.getenv("LAKEFS_HOST", "http://lakefs:8000")
LAKEFS_ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY", "AKIAJBWUDLDFGJY36X3Q")
LAKEFS_SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY", "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")

clt = Client(
    host=LAKEFS_HOST,
    username=LAKEFS_ACCESS_KEY,
    password=LAKEFS_SECRET_KEY,
)

def create_branch(repo_name: str, branch_name: str):
    """Create a new branch in LakeFS."""
    try:
        lakefs.repository(repo_name, clt) \
            .branch(branch_name) \
            .create(source_reference="main")
            
        print(f"✅ Created new branch: {branch_name}")
        return branch_name
    except Exception as e:
        print(f"❌ Failed to create new branch: {e}")
        return None
    

def commit_to_branch(repo_name: str, branch_name: str, commit_message: str):
    """Commit the current branch in LakeFS."""
    try:
        commit = lakefs.repository(repo_name, clt) \
            .branch(branch_name) \
            .commit(message=commit_message)
            
        print(f"✅ Committed branch {branch_name}: {commit.id}")
    except Exception as e:
        print(f"❌ Commit failed: {e}")