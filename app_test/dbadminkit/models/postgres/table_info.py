from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class TableInfo:
    """Configuration for a database table."""
    table_name: str
    key: str = "user_id"
    scd_type: str = "type1"

    def __post_init__(self):
        """Validate fields after initialization."""
        if not self.table_name:
            raise ValueError("table_name is required")
        if self.scd_type not in {"type0", "type1", "type2"}:
            raise ValueError(f"Invalid scd_type: {self.scd_type}")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableInfo":
        """Create TableInfo from a dictionary."""
        return cls(**{k: data.get(k) for k in cls.__dataclass_fields__})