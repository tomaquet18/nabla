#!/usr/bin/env bash
# License gate: every source file starts with the SPDX header of its
# component and every Cargo.toml declares the matching license.
#
#   src/**/*.rs, sql/*.sql        SPDX-License-Identifier: AGPL-3.0-or-later
#   clients/**/*.rs               SPDX-License-Identifier: MIT OR Apache-2.0
#   Cargo.toml                    license = "AGPL-3.0-or-later"
#   clients/rust/nabla-client     license = "MIT OR Apache-2.0"
#
# Runs anywhere (CI and locally): bash scripts/check-licenses.sh
set -u
cd "$(dirname "$0")/.." || exit 1

status=0
problem() { printf 'license check: %s\n' "$1" >&2; status=1; }

# header FILE EXPECTED: the first line must carry the expected SPDX identifier.
header() {
  case "$(head -n 1 "$1")" in
    *"SPDX-License-Identifier: $2"*) ;;
    *) problem "$1: first line must contain SPDX-License-Identifier: $2" ;;
  esac
}

while IFS= read -r f; do header "$f" "AGPL-3.0-or-later"; done < <(find src -name '*.rs' | sort)
while IFS= read -r f; do header "$f" "AGPL-3.0-or-later"; done < <(find sql -name '*.sql' | sort)
while IFS= read -r f; do header "$f" "MIT OR Apache-2.0"; done < <(find clients -name '*.rs' -not -path '*/target/*' | sort)

# manifest FILE EXPECTED: the [package] license field.
manifest() {
  local have
  have=$(grep -E '^license *= *"' "$1" | head -n 1 | sed -E 's/^license *= *"([^"]*)".*/\1/')
  [ "$have" = "$2" ] || problem "$1: license = \"$have\", expected \"$2\""
}
manifest Cargo.toml "AGPL-3.0-or-later"
manifest clients/rust/nabla-client/Cargo.toml "MIT OR Apache-2.0"

if [ "$status" -eq 0 ]; then
  echo "license check: every source file and manifest carries its component's license"
fi
exit "$status"
