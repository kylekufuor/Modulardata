# =============================================================================
# core/services/node_service.py - Node CRUD Operations
# =============================================================================
# Handles version tree operations for the data transformation history.
# =============================================================================

import logging
import math
from typing import Any
from uuid import UUID

from lib.supabase_client import SupabaseClient
from app.exceptions import NodeNotFoundError, SessionNotFoundError
from core.models.profile import DataProfile

logger = logging.getLogger(__name__)


def _clean_for_json(obj: Any) -> Any:
    """
    Recursively clean an object for JSON serialization.
    Converts NaN and Infinity values to None.
    """
    if obj is None:
        return None
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_for_json(item) for item in obj]
    return obj


class NodeService:
    """
    Service for node (version) management.

    Nodes form a tree structure tracking all data transformations:
    - Node 0: Original upload (parent_id = null)
    - Node 1: First transformation (parent_id = Node 0)
    - Node 2: Second transformation (parent_id = Node 1)
    - etc.
    """

    @staticmethod
    def create_node(
        session_id: str | UUID,
        parent_id: str | UUID | None,
        storage_path: str,
        row_count: int,
        column_count: int,
        profile_json: dict | DataProfile | None = None,
        transformation: str | None = None,
        transformation_code: str | None = None,
        preview_rows: list[dict] | None = None,
        node_id: str | None = None,
        step_descriptions: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Create a new node in the version tree.

        Args:
            session_id: Session UUID
            parent_id: Parent node UUID (None for root node)
            storage_path: Path to CSV in storage
            row_count: Number of rows
            column_count: Number of columns
            profile_json: Data profile dict
            transformation: Human-readable transformation description
            transformation_code: The pandas code that was executed
            preview_rows: Preview of first N rows
            node_id: Optional pre-generated node ID (for storage path alignment)
            step_descriptions: List of human-readable descriptions for each transformation step

        Returns:
            Created node dict with id, created_at, etc.

        Raises:
            Exception: If creation fails
        """
        client = SupabaseClient.get_client()

        # Normalize UUIDs to strings
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id
        parent_id_str = str(parent_id) if parent_id and isinstance(parent_id, UUID) else parent_id

        # Handle DataProfile object
        profile_dict = None
        if profile_json:
            if hasattr(profile_json, 'model_dump'):
                profile_dict = profile_json.model_dump()
            else:
                profile_dict = profile_json

        # Clean NaN values for JSON serialization
        profile_dict = _clean_for_json(profile_dict)
        clean_preview_rows = _clean_for_json(preview_rows)

        data = {
            "session_id": session_id_str,
            "parent_id": parent_id_str,
            "storage_path": storage_path,
            "row_count": row_count,
            "column_count": column_count,
            "profile_json": profile_dict,
            "transformation": transformation,
            "transformation_code": transformation_code,
            "preview_rows": clean_preview_rows,
            "step_descriptions": step_descriptions,
        }

        # Add pre-generated node_id if provided
        if node_id:
            data["id"] = node_id

        try:
            response = (
                client.table("nodes")
                .insert(data)
                .execute()
            )

            if response.data:
                node = response.data[0]
                logger.info(f"Created node: {node['id']} for session {session_id_str}")
                return node

            raise Exception("Insert returned no data")

        except Exception as e:
            logger.error(f"Failed to create node: {e}")
            raise

    @staticmethod
    def get_node(node_id: str | UUID) -> dict[str, Any]:
        """
        Get a node by ID.

        Args:
            node_id: Node UUID

        Returns:
            Node dict

        Raises:
            NodeNotFoundError: If node doesn't exist
        """
        node = SupabaseClient.fetch_node(node_id)

        if not node:
            raise NodeNotFoundError(str(node_id))

        return node

    @staticmethod
    def get_current_node(session_id: str | UUID) -> dict[str, Any] | None:
        """
        Get the current (active) node for a session.

        Args:
            session_id: Session UUID

        Returns:
            Current node dict, or None if no data uploaded
        """
        return SupabaseClient.fetch_current_node(session_id)

    @staticmethod
    def get_node_profile(node_id: str | UUID) -> dict[str, Any] | None:
        """
        Get just the profile_json for a node.

        Args:
            node_id: Node UUID

        Returns:
            Profile dict, or None if not found
        """
        return SupabaseClient.fetch_node_profile(node_id)

    @staticmethod
    def get_node_history(session_id: str | UUID) -> list[dict[str, Any]]:
        """
        Get all nodes for a session in chronological order.

        Args:
            session_id: Session UUID

        Returns:
            List of node dicts ordered by created_at
        """
        client = SupabaseClient.get_client()
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id

        try:
            response = (
                client.table("nodes")
                .select("*")
                .eq("session_id", session_id_str)
                .order("created_at", desc=False)
                .execute()
            )

            return response.data or []

        except Exception as e:
            logger.error(f"Failed to get node history: {e}")
            return []

    @staticmethod
    def get_node_lineage(node_id: str | UUID, depth: int = 10) -> list[dict[str, Any]]:
        """
        Get the parent chain (lineage) of a node.

        Useful for understanding transformation history and undo.

        Args:
            node_id: Starting node UUID
            depth: How many ancestors to fetch

        Returns:
            List of nodes from oldest ancestor to current
        """
        return SupabaseClient.fetch_node_lineage(node_id, depth)

    @staticmethod
    def get_root_node(session_id: str | UUID) -> dict[str, Any] | None:
        """
        Get the root node (Node 0) for a session.

        Args:
            session_id: Session UUID

        Returns:
            Root node dict, or None if not found
        """
        client = SupabaseClient.get_client()
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id

        try:
            response = (
                client.table("nodes")
                .select("*")
                .eq("session_id", session_id_str)
                .is_("parent_id", "null")
                .single()
                .execute()
            )

            return response.data

        except Exception as e:
            # Could be no root node yet
            logger.debug(f"No root node found for session {session_id_str}")
            return None

    @staticmethod
    def get_children(node_id: str | UUID) -> list[dict[str, Any]]:
        """
        Get all child nodes of a given node.

        Useful for understanding branching after undo/rollback.

        Args:
            node_id: Parent node UUID

        Returns:
            List of child node dicts
        """
        client = SupabaseClient.get_client()
        node_id_str = str(node_id) if isinstance(node_id, UUID) else node_id

        try:
            response = (
                client.table("nodes")
                .select("*")
                .eq("parent_id", node_id_str)
                .order("created_at", desc=False)
                .execute()
            )

            return response.data or []

        except Exception as e:
            logger.error(f"Failed to get children: {e}")
            return []

    @staticmethod
    def update_node(
        node_id: str | UUID,
        profile_json: dict | None = None,
        transformation: str | None = None,
        transformation_code: str | None = None,
    ) -> dict[str, Any]:
        """
        Update a node's metadata.

        Args:
            node_id: Node UUID
            profile_json: New profile dict
            transformation: New transformation description
            transformation_code: New code string

        Returns:
            Updated node dict

        Raises:
            NodeNotFoundError: If node doesn't exist
        """
        # Verify node exists
        NodeService.get_node(node_id)

        client = SupabaseClient.get_client()
        node_id_str = str(node_id) if isinstance(node_id, UUID) else node_id

        update_data = {}
        if profile_json is not None:
            update_data["profile_json"] = profile_json
        if transformation is not None:
            update_data["transformation"] = transformation
        if transformation_code is not None:
            update_data["transformation_code"] = transformation_code

        if not update_data:
            return NodeService.get_node(node_id)

        try:
            response = (
                client.table("nodes")
                .update(update_data)
                .eq("id", node_id_str)
                .execute()
            )

            if response.data:
                logger.info(f"Updated node: {node_id_str}")
                return response.data[0]

            return NodeService.get_node(node_id)

        except Exception as e:
            logger.error(f"Failed to update node: {e}")
            raise

    @staticmethod
    def count_nodes(session_id: str | UUID) -> int:
        """
        Count total nodes for a session.

        Args:
            session_id: Session UUID

        Returns:
            Number of nodes
        """
        client = SupabaseClient.get_client()
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id

        try:
            response = (
                client.table("nodes")
                .select("*", count="exact")
                .eq("session_id", session_id_str)
                .execute()
            )

            return response.count or 0

        except Exception as e:
            logger.error(f"Failed to count nodes: {e}")
            return 0

    @staticmethod
    def delete_all_nodes(session_id: str | UUID) -> int:
        """
        Delete all nodes for a session.

        Used when replacing data (uploading a new file to an existing session).
        This is a destructive operation.

        Args:
            session_id: Session UUID

        Returns:
            Number of nodes deleted
        """
        from core.services.storage_service import StorageService

        client = SupabaseClient.get_client()
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id

        # Get all nodes for the session
        nodes = NodeService.get_node_history(session_id_str)

        if not nodes:
            return 0

        # Delete storage files for each node
        for node in nodes:
            storage_path = node.get("storage_path")
            if storage_path:
                try:
                    StorageService.delete_file(storage_path)
                except Exception as e:
                    logger.warning(f"Failed to delete storage file {storage_path}: {e}")

        # Delete all nodes in one query
        try:
            client.table("nodes").delete().eq("session_id", session_id_str).execute()
            logger.info(f"Deleted {len(nodes)} nodes for session {session_id_str}")
            return len(nodes)
        except Exception as e:
            logger.error(f"Failed to delete nodes: {e}")
            raise

    @staticmethod
    def delete_node(node_id: str | UUID, session_id: str | UUID) -> dict[str, Any]:
        """
        Delete a node and its associated storage.

        This is a destructive operation that removes the node permanently.
        Only the current (latest) node can be deleted, and it must have a parent.

        Args:
            node_id: Node UUID to delete
            session_id: Session UUID (for verification)

        Returns:
            Dict with parent_id that should become the new current node

        Raises:
            NodeNotFoundError: If node doesn't exist
            ValueError: If node cannot be deleted (has children, is root, etc.)
        """
        from core.services.storage_service import StorageService

        client = SupabaseClient.get_client()
        node_id_str = str(node_id) if isinstance(node_id, UUID) else node_id
        session_id_str = str(session_id) if isinstance(session_id, UUID) else session_id

        # Get the node
        node = NodeService.get_node(node_id_str)

        # Verify node belongs to session
        if node.get("session_id") != session_id_str:
            raise ValueError("Node does not belong to this session")

        # Verify node has a parent (can't delete original data)
        parent_id = node.get("parent_id")
        if not parent_id:
            raise ValueError("Cannot delete the original data node")

        # Verify node has no children (must be a leaf node)
        children = NodeService.get_children(node_id_str)
        if children:
            raise ValueError("Cannot delete node with children. Delete descendants first.")

        # Delete the CSV file from storage
        storage_path = node.get("storage_path")
        if storage_path:
            try:
                StorageService.delete_file(storage_path)
                logger.info(f"Deleted storage file: {storage_path}")
            except Exception as e:
                logger.warning(f"Failed to delete storage file {storage_path}: {e}")

        # Delete the node from database
        try:
            client.table("nodes").delete().eq("id", node_id_str).execute()
            logger.info(f"Deleted node: {node_id_str}")
        except Exception as e:
            logger.error(f"Failed to delete node: {e}")
            raise

        return {
            "deleted_node_id": node_id_str,
            "parent_id": parent_id,
            "transformation": node.get("transformation"),
        }
