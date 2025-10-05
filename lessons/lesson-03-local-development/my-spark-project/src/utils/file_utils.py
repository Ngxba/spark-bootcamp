"""
File utility functions for data processing pipelines.
This module provides helper functions for file operations and path management.
"""

import os
import json
import yaml
from typing import Dict, Any, List, Optional
from pathlib import Path


class FileUtils:
    """
    Utility class for file operations commonly used in data pipelines.

    This class provides:
    - File existence checks
    - Configuration file reading
    - Path utilities
    - File system operations
    """

    @staticmethod
    def file_exists(file_path: str) -> bool:
        """
        Check if a file exists.

        Args:
            file_path: Path to the file

        Returns:
            True if file exists, False otherwise
        """
        return os.path.isfile(file_path)

    @staticmethod
    def directory_exists(dir_path: str) -> bool:
        """
        Check if a directory exists.

        Args:
            dir_path: Path to the directory

        Returns:
            True if directory exists, False otherwise
        """
        return os.path.isdir(dir_path)

    @staticmethod
    def create_directory(dir_path: str, exist_ok: bool = True) -> None:
        """
        Create a directory if it doesn't exist.

        Args:
            dir_path: Path to the directory
            exist_ok: If True, don't raise error if directory already exists
        """
        Path(dir_path).mkdir(parents=True, exist_ok=exist_ok)

    @staticmethod
    def get_file_size(file_path: str) -> int:
        """
        Get file size in bytes.

        Args:
            file_path: Path to the file

        Returns:
            File size in bytes
        """
        if FileUtils.file_exists(file_path):
            return os.path.getsize(file_path)
        return 0

    @staticmethod
    def list_files_in_directory(
        dir_path: str, extension: Optional[str] = None, recursive: bool = False
    ) -> List[str]:
        """
        List files in a directory.

        Args:
            dir_path: Path to the directory
            extension: File extension to filter by (e.g., '.csv', '.json')
            recursive: Whether to search recursively

        Returns:
            List of file paths
        """
        if not FileUtils.directory_exists(dir_path):
            return []

        files = []
        path = Path(dir_path)

        if recursive:
            pattern = f"**/*{extension}" if extension else "**/*"
            files = [str(f) for f in path.glob(pattern) if f.is_file()]
        else:
            pattern = f"*{extension}" if extension else "*"
            files = [str(f) for f in path.glob(pattern) if f.is_file()]

        return sorted(files)

    @staticmethod
    def read_json_file(file_path: str) -> Dict[str, Any]:
        """
        Read JSON file and return as dictionary.

        Args:
            file_path: Path to the JSON file

        Returns:
            Dictionary containing JSON data

        Raises:
            FileNotFoundError: If file doesn't exist
            json.JSONDecodeError: If file is not valid JSON
        """
        if not FileUtils.file_exists(file_path):
            raise FileNotFoundError(f"JSON file not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def write_json_file(data: Dict[str, Any], file_path: str, indent: int = 2) -> None:
        """
        Write dictionary to JSON file.

        Args:
            data: Dictionary to write
            file_path: Path to the output JSON file
            indent: JSON indentation level
        """
        # Create directory if it doesn't exist
        dir_path = os.path.dirname(file_path)
        if dir_path:
            FileUtils.create_directory(dir_path)

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=indent, ensure_ascii=False)

    @staticmethod
    def read_yaml_file(file_path: str) -> Dict[str, Any]:
        """
        Read YAML file and return as dictionary.

        Args:
            file_path: Path to the YAML file

        Returns:
            Dictionary containing YAML data

        Raises:
            FileNotFoundError: If file doesn't exist
            yaml.YAMLError: If file is not valid YAML
        """
        if not FileUtils.file_exists(file_path):
            raise FileNotFoundError(f"YAML file not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as file:
            return yaml.safe_load(file)

    @staticmethod
    def write_yaml_file(data: Dict[str, Any], file_path: str) -> None:
        """
        Write dictionary to YAML file.

        Args:
            data: Dictionary to write
            file_path: Path to the output YAML file
        """
        # Create directory if it doesn't exist
        dir_path = os.path.dirname(file_path)
        if dir_path:
            FileUtils.create_directory(dir_path)

        with open(file_path, "w", encoding="utf-8") as file:
            yaml.dump(data, file, default_flow_style=False, indent=2)

    @staticmethod
    def read_text_file(file_path: str, encoding: str = "utf-8") -> str:
        """
        Read text file content.

        Args:
            file_path: Path to the text file
            encoding: File encoding

        Returns:
            File content as string

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        if not FileUtils.file_exists(file_path):
            raise FileNotFoundError(f"Text file not found: {file_path}")

        with open(file_path, "r", encoding=encoding) as file:
            return file.read()

    @staticmethod
    def write_text_file(content: str, file_path: str, encoding: str = "utf-8") -> None:
        """
        Write content to text file.

        Args:
            content: Content to write
            file_path: Path to the output text file
            encoding: File encoding
        """
        # Create directory if it doesn't exist
        dir_path = os.path.dirname(file_path)
        if dir_path:
            FileUtils.create_directory(dir_path)

        with open(file_path, "w", encoding=encoding) as file:
            file.write(content)

    @staticmethod
    def copy_file(source_path: str, destination_path: str) -> None:
        """
        Copy file from source to destination.

        Args:
            source_path: Source file path
            destination_path: Destination file path

        Raises:
            FileNotFoundError: If source file doesn't exist
        """
        import shutil

        if not FileUtils.file_exists(source_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")

        # Create destination directory if it doesn't exist
        dest_dir = os.path.dirname(destination_path)
        if dest_dir:
            FileUtils.create_directory(dest_dir)

        shutil.copy2(source_path, destination_path)

    @staticmethod
    def delete_file(file_path: str, ignore_errors: bool = True) -> bool:
        """
        Delete a file.

        Args:
            file_path: Path to the file to delete
            ignore_errors: Whether to ignore errors if file doesn't exist

        Returns:
            True if file was deleted or didn't exist, False if error occurred
        """
        try:
            if FileUtils.file_exists(file_path):
                os.remove(file_path)
                return True
            return True  # File didn't exist, consider it "deleted"
        except Exception:
            if ignore_errors:
                return False
            raise

    @staticmethod
    def get_absolute_path(file_path: str) -> str:
        """
        Get absolute path for a file.

        Args:
            file_path: Relative or absolute file path

        Returns:
            Absolute file path
        """
        return os.path.abspath(file_path)

    @staticmethod
    def get_parent_directory(file_path: str) -> str:
        """
        Get parent directory of a file.

        Args:
            file_path: File path

        Returns:
            Parent directory path
        """
        return os.path.dirname(file_path)

    @staticmethod
    def get_filename(file_path: str, include_extension: bool = True) -> str:
        """
        Get filename from a file path.

        Args:
            file_path: File path
            include_extension: Whether to include file extension

        Returns:
            Filename
        """
        filename = os.path.basename(file_path)
        if not include_extension and "." in filename:
            filename = filename.rsplit(".", 1)[0]
        return filename

    @staticmethod
    def get_file_extension(file_path: str) -> str:
        """
        Get file extension.

        Args:
            file_path: File path

        Returns:
            File extension (including the dot)
        """
        return os.path.splitext(file_path)[1]

    @staticmethod
    def join_paths(*paths: str) -> str:
        """
        Join multiple path components.

        Args:
            *paths: Path components to join

        Returns:
            Joined path
        """
        return os.path.join(*paths)

    @staticmethod
    def normalize_path(file_path: str) -> str:
        """
        Normalize a file path.

        Args:
            file_path: File path to normalize

        Returns:
            Normalized file path
        """
        return os.path.normpath(file_path)

    @staticmethod
    def get_files_by_pattern(directory: str, pattern: str) -> List[str]:
        """
        Get files matching a glob pattern.

        Args:
            directory: Directory to search in
            pattern: Glob pattern (e.g., '*.csv', '**/*.json')

        Returns:
            List of matching file paths
        """
        import glob

        search_pattern = FileUtils.join_paths(directory, pattern)
        return sorted(glob.glob(search_pattern, recursive="**" in pattern))

    @staticmethod
    def ensure_file_path_exists(file_path: str) -> str:
        """
        Ensure the directory for a file path exists.

        Args:
            file_path: File path

        Returns:
            The same file path
        """
        dir_path = FileUtils.get_parent_directory(file_path)
        if dir_path:
            FileUtils.create_directory(dir_path)
        return file_path

    @staticmethod
    def is_empty_directory(dir_path: str) -> bool:
        """
        Check if a directory is empty.

        Args:
            dir_path: Directory path

        Returns:
            True if directory is empty or doesn't exist
        """
        if not FileUtils.directory_exists(dir_path):
            return True
        return len(os.listdir(dir_path)) == 0

    @staticmethod
    def get_file_info(file_path: str) -> Dict[str, Any]:
        """
        Get comprehensive file information.

        Args:
            file_path: File path

        Returns:
            Dictionary with file information
        """
        if not FileUtils.file_exists(file_path):
            return {"exists": False}

        stat = os.stat(file_path)
        return {
            "exists": True,
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "created_time": stat.st_ctime,
            "modified_time": stat.st_mtime,
            "accessed_time": stat.st_atime,
            "extension": FileUtils.get_file_extension(file_path),
            "filename": FileUtils.get_filename(file_path),
            "directory": FileUtils.get_parent_directory(file_path),
            "absolute_path": FileUtils.get_absolute_path(file_path),
        }
