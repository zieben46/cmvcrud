# datastorekit/exceptions.py
class DatastoreError(Exception):
    """Base exception for datastore-related errors."""
    pass

class DuplicateKeyError(DatastoreError):
    """Raised when a duplicate key violation occurs."""
    pass

class NullValueError(DatastoreError):
    """Raised when a null value is inserted into a non-nullable field."""
    pass

class DatastoreOperationError(DatastoreError):
    """Raised for general database operation errors."""
    pass