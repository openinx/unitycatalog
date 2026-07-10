#!/usr/bin/env python3
"""Guards against version drift between the docs and the build configuration.

The documentation hardcodes Maven coordinates such as
``io.unitycatalog:unitycatalog-spark_4.0_2.13:0.5.0`` in many places. Those
strings silently go stale whenever the project version or the set of supported
Spark versions changes, which is exactly the drift this check prevents.

It validates, across every Markdown file under ``docs/``:

1. The Unity Catalog version in ``unitycatalog-spark`` coordinates matches the
   release form of ``version.sbt`` (``0.5.0-SNAPSHOT`` -> ``0.5.0``).
2. The Spark-version suffix in ``unitycatalog-spark`` / ``delta-spark``
   coordinates and in ``-DsparkVersion=`` flags is one the build still supports,
   per ``project/spark-versions.json``.

The Delta Lake and Hadoop version numbers are intentionally not checked here:
they track external projects and are not defined in the build configuration, so
they are not a source of truth this check can enforce.

Usage:
    python3 project/tests/test_docs_versions.py

Exits 0 when the docs are in sync, 1 (with a per-location report) otherwise.
"""

import json
import re
import sys
from pathlib import Path

UC_ROOT = Path(__file__).resolve().parent.parent.parent
DOCS_DIR = UC_ROOT / "docs"

# io.unitycatalog:unitycatalog-spark[_<spark>]_2.13:<uc-version>
UC_SPARK_COORD = re.compile(
    r"io\.unitycatalog:unitycatalog-spark(?:_(\d+\.\d+))?_2\.13:([^\s\",\\]+)"
)
# io.delta:delta-spark[_<spark>]_2.13:<delta-version>
DELTA_SPARK_COORD = re.compile(
    r"io\.delta:delta-spark(?:_(\d+\.\d+))?_2\.13:[^\s\",\\]+"
)
# build/sbt -DsparkVersion=<spark>
SPARK_VERSION_FLAG = re.compile(r"-DsparkVersion=(\d+\.\d+)")


def uc_release_version() -> str:
    """Returns the release form of the project version (drops -SNAPSHOT)."""
    version_re = re.compile(r'version\s*:=\s*"([^"]+)"')
    for line in (UC_ROOT / "version.sbt").read_text().splitlines():
        match = version_re.search(line)
        if match:
            return match.group(1).replace("-SNAPSHOT", "")
    sys.exit("Error: could not parse version from version.sbt")


def supported_spark_shorts() -> set:
    """Returns the supported Spark short versions, e.g. {'4.0', '4.1', '4.2'}."""
    data = json.loads((UC_ROOT / "project" / "spark-versions.json").read_text())
    return {".".join(entry["version"].split(".")[:2]) for entry in data["versions"]}


def check_docs(uc_version: str, spark_shorts: set) -> list:
    errors = []
    allowed = ", ".join(sorted(spark_shorts))
    for md_file in sorted(DOCS_DIR.rglob("*.md")):
        rel = md_file.relative_to(UC_ROOT)
        for lineno, line in enumerate(md_file.read_text().splitlines(), start=1):
            for spark_short, version in UC_SPARK_COORD.findall(line):
                if version != uc_version:
                    errors.append(
                        f"{rel}:{lineno}: unitycatalog-spark version '{version}' "
                        f"does not match version.sbt release '{uc_version}'"
                    )
                if spark_short and spark_short not in spark_shorts:
                    errors.append(
                        f"{rel}:{lineno}: unitycatalog-spark targets unsupported "
                        f"Spark '{spark_short}' (supported: {allowed})"
                    )
            for spark_short in DELTA_SPARK_COORD.findall(line):
                if spark_short and spark_short not in spark_shorts:
                    errors.append(
                        f"{rel}:{lineno}: delta-spark targets unsupported "
                        f"Spark '{spark_short}' (supported: {allowed})"
                    )
            for spark_short in SPARK_VERSION_FLAG.findall(line):
                if spark_short not in spark_shorts:
                    errors.append(
                        f"{rel}:{lineno}: -DsparkVersion={spark_short} is not a "
                        f"supported Spark version (supported: {allowed})"
                    )
    return errors


def main() -> None:
    if not DOCS_DIR.is_dir():
        sys.exit(f"Error: docs directory not found at {DOCS_DIR}")

    uc_version = uc_release_version()
    spark_shorts = supported_spark_shorts()

    print("Checking docs against build configuration")
    print(f"  Unity Catalog release version : {uc_version}")
    print(f"  Supported Spark versions      : {', '.join(sorted(spark_shorts))}")

    errors = check_docs(uc_version, spark_shorts)
    if errors:
        print(f"\nFAIL: found {len(errors)} version mismatch(es):")
        for error in errors:
            print(f"  {error}")
        print(
            "\nUpdate the docs to match version.sbt / project/spark-versions.json, "
            "or update those sources if the docs are correct."
        )
        sys.exit(1)

    print("\nPASS: docs version strings are in sync with the build configuration.")


if __name__ == "__main__":
    main()
