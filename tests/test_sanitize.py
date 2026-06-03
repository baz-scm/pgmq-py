"""Unit tests for sanitize_nul (no database required)."""

from pgmq_py.utils import sanitize_nul


class TestSanitizeNul:
    def test_replaces_raw_nul_in_string(self) -> None:
        assert sanitize_nul("a\x00b") == "a\\u0000b"

    def test_leaves_escaped_literal_untouched(self) -> None:
        assert sanitize_nul("a\\u0000b") == "a\\u0000b"

    def test_recurses_into_nested_dict_and_list(self) -> None:
        value = {"outer": {"items": ["x\x00y", "z"]}}
        assert sanitize_nul(value) == {"outer": {"items": ["x\\u0000y", "z"]}}

    def test_non_string_values_pass_through(self) -> None:
        assert sanitize_nul(42) == 42
        assert sanitize_nul(None) is None
