import re
import subprocess
import pathlib
from i18n import TEXT

pattern = re.compile(r"""(?<!\w)t\s*\(\s*['\"]([^'\"]+)['\"]""")


def git_tracked_py_files():
    # Only files tracked by git => excludes venv/.git automatically
    out = subprocess.check_output(["git", "ls-files", "*.py"], text=True)
    return [pathlib.Path(line.strip()) for line in out.splitlines() if line.strip()]


used = set()
for p in git_tracked_py_files():
    s = p.read_text(encoding="utf-8", errors="ignore")
    for m in pattern.finditer(s):
        used.add(m.group(1))

missing = {lang: sorted(k for k in used if k not in TEXT.get(lang, {})) for lang in ("ru", "en", "uk")}

any_missing = False
for lang, keys in missing.items():
    if keys:
        any_missing = True
        print(f"\nMISSING in {lang}: {len(keys)}")
        for k in keys[:200]:
            print(" ", k)
        if len(keys) > 200:
            print("  ...")

if not any_missing:
    print("OK: No missing i18n keys for ru/en/uk.")
