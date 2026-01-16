# =============================================================================
# core/services/storage_service.py - Supabase Storage Operations
# =============================================================================
# Handles file upload/download operations with Supabase Storage.
# =============================================================================

import io
import logging
from typing import BinaryIO

import pandas as pd

from lib.supabase_client import SupabaseClient
from app.config import settings
from app.exceptions import StorageUploadError, StorageDownloadError

logger = logging.getLogger(__name__)

# Storage bucket name
BUCKET_NAME = "uploads"


class StorageService:
    """
    Service for Supabase Storage operations.

    Handles uploading and downloading CSV files to/from storage.
    """

    @staticmethod
    def upload_csv(
        session_id: str,
        df: pd.DataFrame,
        filename: str = "data.csv",
        node_id: str | None = None,
        path_override: str | None = None,
    ) -> str:
        """
        Upload a DataFrame as CSV to Supabase Storage.

        Args:
            session_id: Session UUID
            df: DataFrame to upload
            filename: Filename to use
            node_id: Optional node ID for path
            path_override: Optional full path (overrides other path logic)

        Returns:
            Storage path where file was uploaded

        Raises:
            StorageUploadError: If upload fails
        """
        client = SupabaseClient.get_client()

        # Build storage path: sessions/{session_id}/{node_id or filename}
        if path_override:
            path = path_override
        elif node_id:
            path = f"sessions/{session_id}/{node_id}.csv"
        else:
            path = f"sessions/{session_id}/{filename}"

        # Convert DataFrame to CSV bytes
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        try:
            # Upload to storage
            response = client.storage.from_(BUCKET_NAME).upload(
                path=path,
                file=csv_buffer.getvalue(),
                file_options={"content-type": "text/csv", "upsert": "true"}
            )

            logger.info(f"Uploaded file to storage: {path}")
            return path

        except Exception as e:
            logger.error(f"Storage upload failed: {e}")
            raise StorageUploadError(str(e))

    @staticmethod
    def upload_file(
        session_id: str,
        file_content: bytes,
        filename: str,
    ) -> str:
        """
        Upload raw file content to storage.

        Args:
            session_id: Session UUID
            file_content: File bytes
            filename: Original filename

        Returns:
            Storage path

        Raises:
            StorageUploadError: If upload fails
        """
        client = SupabaseClient.get_client()

        # Build storage path
        path = f"sessions/{session_id}/original_{filename}"

        try:
            response = client.storage.from_(BUCKET_NAME).upload(
                path=path,
                file=file_content,
                file_options={"content-type": "text/csv", "upsert": "true"}
            )

            logger.info(f"Uploaded raw file to storage: {path}")
            return path

        except Exception as e:
            logger.error(f"Storage upload failed: {e}")
            raise StorageUploadError(str(e))

    @staticmethod
    def download_csv(storage_path: str) -> pd.DataFrame:
        """
        Download a CSV file from storage and return as DataFrame.

        Args:
            storage_path: Path in storage bucket

        Returns:
            DataFrame with file contents

        Raises:
            StorageDownloadError: If download fails
        """
        client = SupabaseClient.get_client()

        try:
            # Download file content
            response = client.storage.from_(BUCKET_NAME).download(storage_path)

            # Parse CSV
            csv_buffer = io.BytesIO(response)
            df = pd.read_csv(csv_buffer)

            logger.info(f"Downloaded file from storage: {storage_path} ({len(df)} rows)")
            return df

        except Exception as e:
            logger.error(f"Storage download failed: {e}")
            raise StorageDownloadError(storage_path, str(e))

    @staticmethod
    def download_raw(storage_path: str) -> bytes:
        """
        Download raw file content from storage.

        Args:
            storage_path: Path in storage bucket

        Returns:
            File content as bytes

        Raises:
            StorageDownloadError: If download fails
        """
        client = SupabaseClient.get_client()

        try:
            response = client.storage.from_(BUCKET_NAME).download(storage_path)
            logger.info(f"Downloaded raw file from storage: {storage_path}")
            return response

        except Exception as e:
            logger.error(f"Storage download failed: {e}")
            raise StorageDownloadError(storage_path, str(e))

    @staticmethod
    def get_public_url(storage_path: str) -> str:
        """
        Get a public URL for a storage file.

        Args:
            storage_path: Path in storage bucket

        Returns:
            Public URL string
        """
        client = SupabaseClient.get_client()

        try:
            result = client.storage.from_(BUCKET_NAME).get_public_url(storage_path)
            return result
        except Exception as e:
            logger.error(f"Failed to get public URL: {e}")
            raise

    @staticmethod
    def delete_file(storage_path: str) -> bool:
        """
        Delete a file from storage.

        Args:
            storage_path: Path in storage bucket

        Returns:
            True if deleted successfully
        """
        client = SupabaseClient.get_client()

        try:
            client.storage.from_(BUCKET_NAME).remove([storage_path])
            logger.info(f"Deleted file from storage: {storage_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete file: {e}")
            return False

    @staticmethod
    def list_files(session_id: str) -> list[dict]:
        """
        List all files for a session.

        Args:
            session_id: Session UUID

        Returns:
            List of file info dicts
        """
        client = SupabaseClient.get_client()
        path = f"sessions/{session_id}"

        try:
            response = client.storage.from_(BUCKET_NAME).list(path)
            return response or []

        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return []
