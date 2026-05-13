from airflow.exceptions import AirflowFailException

from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path


class StorageClient(ABC):
    """
    Abstract base class defining the interface for storage client operations with multiple storage
    backends (Local File System, GCS, AWS S3, ADLS Gen2, etc.).
    """

    @abstractmethod
    def save(self, data: bytes, file_path: str) -> None:
        """Saves raw bytes data to the specified path."""
        pass

    @abstractmethod
    def load(self, file_path: str) -> bytes:
        """Loads raw bytes data from the specified file path."""
        pass

    @abstractmethod
    def file_exists(self, file_path: str) -> bool:
        """Checks if a file exists at specified path."""
        pass

    @abstractmethod
    def save_df(self, df: pd.DataFrame, file_path: str, **kwargs) -> None:
        """Saves a pandas DataFrame to the specified path based on file extension."""
        pass


class LocalStorageClient(StorageClient):
    """A storage client implementation for interacting with the local file system."""

    def save(self, data: bytes, file_path: str) -> None:
        """Saves raw bytes data to the specified path."""
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "wb") as f:
            f.write(data)

    def load(self, file_path: str) -> bytes:
        """Loads raw bytes data from the specified file path."""
        if not self.file_exists(file_path):
            raise AirflowFailException(f"Specified file: {file_path} does not exist")
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()

    def file_exists(self, file_path: str) -> bool:
        """Checks if a file exists at the given path."""
        return Path(file_path).exists()

    def save_df(self, df: pd.DataFrame, file_path: str, **kwargs) -> None:
        """Saves a pandas DataFrame to the specified path based on file extension."""
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        extension = path.suffix.lower()

        if extension == ".parquet":
            df.to_parquet(file_path, engine="pyarrow", index=False, **kwargs)
        else:  # More file formats handling can be added here in the future if needed
            raise ValueError(f"Unsupported file extension: {extension}")


class GCSStorageClient(StorageClient):
    """A storage client implementation for interacting with Google Cloud Storage (GCS)."""

    def save(self, data: bytes, file_path: str) -> None:
        """Saves raw bytes data to the specified path."""
        raise NotImplementedError("Method is not implemented yet")

    def load(self, file_path: str) -> bytes:
        """Loads raw bytes data from the specified file path."""
        if not self.file_exists(file_path):
            raise AirflowFailException(f"Specified file: {file_path} does not exist")
        raise NotImplementedError("Method is not implemented yet")

    def file_exists(self, file_path: str) -> bool:
        """Checks if a file exists at the given path."""
        raise NotImplementedError("Method is not implemented yet")

    def save_df(self, df: pd.DataFrame, file_path: str, **kwargs) -> None:
        """Saves a pandas DataFrame to the specified path based on file extension."""
        raise NotImplementedError("Method is not implemented yet")
