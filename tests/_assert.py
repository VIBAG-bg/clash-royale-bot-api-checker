"""Domain assertions for report text checks."""

from __future__ import annotations

import re

from i18n import t


def _extract_section(report: str, header: str) -> list[str]:
    lines = report.splitlines()
    if header not in lines:
        raise AssertionError(f"Missing section header: {header}")
    start = lines.index(header) + 1
    section: list[str] = []
    for line in lines[start:]:
        if not line.strip():
            break
        section.append(line)
    return section


def assert_has_section(report: str, header_key: str, lang: str = "en") -> None:
    header = t(header_key, lang)
    if header not in report.splitlines():
        raise AssertionError(f"Section not found: {header_key} ({header})")


def assert_player_count_in_section(
    report: str,
    header_key: str,
    expected_n: int,
    lang: str = "en",
) -> None:
    header = t(header_key, lang)
    section_lines = _extract_section(report, header)
    count = sum(1 for line in section_lines if re.match(r"^\d+\)", line))
    if count != expected_n:
        raise AssertionError(
            f"Section {header_key} expected {expected_n} players, got {count}"
        )


def assert_no_duplicates_between_sections(
    report: str,
    header_keys: list[str],
    lang: str = "en",
) -> None:
    seen: set[str] = set()
    for header_key in header_keys:
        header = t(header_key, lang)
        lines = _extract_section(report, header)
        for line in lines:
            if not re.match(r"^\d+\)", line):
                continue
            marker = line.strip().lower()
            if marker in seen:
                raise AssertionError(
                    f"Duplicate player line found across sections: {line}"
                )
            seen.add(marker)
