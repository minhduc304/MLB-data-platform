from abc import ABC, abstractmethod
from typing import Optional, List, TypeVar, Generic

T = TypeVar('T')

class BaseRepository(ABC, Generic[T]):
    """Abstract base for all repositories."""

    @abstractmethod
    def get_by_id(self, entity_id: int) -> Optional[T]:
        """Retrieve a single entity by ID"""

    @abstractmethod
    def get_all(self) -> List[T]:
        """Retrieve all entities."""
        pass

    @abstractmethod
    def save(self, entity: T) -> None:
        """Save an entity (insert or update)."""
        pass

    @abstractmethod
    def delete(self, entity_id: int) -> bool:
        """Delete an entity. Returns True if deleted."""
        pass

    @abstractmethod
    def exists(self, entity_id: int) -> bool:
        """Check if entity exists."""
        pass
