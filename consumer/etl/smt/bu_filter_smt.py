
# consumer/etl/smt/bu_filter_smt.py

from __future__ import annotations
from typing import Optional, Dict, Any, Iterable, Set

from .base_smt import BaseSMT


class BusinessUnitFilterSMT(BaseSMT):
    """
    Business Unit Filter SMT:
    - Config-driven allowlist of business units.
    - Drops records where:
        - business_unit field is missing OR empty, OR
        - business_unit not in allowed set.
    - Supports alternate field names (e.g., 'business_unit_id').

    Expected config example:
        {
            "allowed_business_units": ["BU1", "BU2"],
            # Optional:
            "field_names": ["business_unit", "business_unit_id"],
            "normalize_case": "upper"  # one of: None | "upper" | "lower"
        }
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Store allowed BUs from config into a set for O(1) lookup.
        Optionally support case normalization and multiple field names.
        """
        if not isinstance(config, dict):
            raise ValueError("BusinessUnitFilterSMT config must be a dict.")

        allowed: Iterable[str] = config.get("allowed_business_units", [])
        if not allowed:
            raise ValueError(
                "BusinessUnitFilterSMT requires 'allowed_business_units' in config."
            )

        self.normalize_case: Optional[str] = config.get("normalize_case")
        if self.normalize_case not in (None, "upper", "lower"):
            raise ValueError("normalize_case must be one of: None, 'upper', 'lower'.")

        # Normalize allowlist based on normalization setting
        self.allowed_business_units: Set[str] = set(
            self._normalize_value(bu) for bu in allowed if bu is not None
        )

        # Field names to inspect for business unit
        field_names: Iterable[str] = config.get(
            "field_names", ("business_unit", "business_unit_id")
        )
        self.field_names: tuple[str, ...] = tuple(field_names)

    def apply(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Steps to implement:
        1. read bu = record.get("business_unit") or record.get("business_unit_id")
        2. if bu is None or empty string: return None  # drop
        3. if bu not in allowed set (after normalization): return None  # drop
        4. else: return record  # pass downstream
        """
        if not isinstance(record, dict):
            # If the pipeline passes an unexpected type, drop safely.
            return None

        bu_value = None
        for field in self.field_names:
            if field in record:
                bu_value = record.get(field)
                break

        # Drop if missing or empty
        if bu_value is None or (isinstance(bu_value, str) and bu_value.strip() == ""):
            return None

        normalized_bu = self._normalize_value(bu_value)

        # Drop if not in allowlist
        if normalized_bu not in self.allowed_business_units:
            return None

        # Optionally, you could write back the normalized value into the record
        # to ensure consistency downstream. Commented out by default:
        # if isinstance(bu_value, str) and bu_value != normalized_bu:
        #     record[self._primary_field_name()] = normalized_bu

        return record

    # ---------- Helpers ----------

    def _normalize_value(self, value: Any) -> Any:
        """
        Normalize the business unit value according to 'normalize_case' setting.
        Only applies to strings; other types are returned unchanged.
        """
        if self.normalize_case == "upper" and isinstance(value, str):
            return value.upper()
        if self.normalize_case == "lower" and isinstance(value, str):
            return value.lower()
        return value

    def _primary_field_name(self) -> str:
        """
        Return the first field name in the configured list as the canonical BU field.
        """
        return self.field_names[0] if self.field_names else "business_unit"
