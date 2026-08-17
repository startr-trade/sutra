#!/usr/bin/env bash
#
# Run the `sutra` commands the documentation tells a reader to run, and report which ones work.
#
# WHY THIS EXISTS: the getting-started path is the first thing a new reader executes and the
# last thing anyone re-reads. `sutra create app` followed by `sutra package` — the two commands
# the scaffolder itself prints as "next" — were broken in 0.2.0-rc.1 by four separate defects,
# and every unit test in the tree passed anyway. Prose that names a command is a promise; this
# checks the promises.
#
# WHAT IT DOES: extracts every `sutra …` invocation from the fenced code blocks of the
# getting-started chapters (plus any chapter named on the command line), runs each in a scratch
# workspace in document order, and prints a table. Placeholders are substituted, not guessed at:
# a command containing an unresolvable one is SKIPPED and reported as such rather than failed.
#
# WHAT IT DOES NOT RUN, deliberately — these need a live estate, and a false failure here would
# be worse than no check:
#   * anything that talks to a cluster or a running engine (deploy, undeploy, migrate)
#   * docker / docker compose / kubectl lines (the image may not even be pullable yet)
#
# Usage:
#   SUTRA_BIN=/path/to/sutra bash scripts/verify-doc-commands.sh [chapter.md …]
#   make verify-docs
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
BIN="${SUTRA_BIN:-$(command -v sutra || true)}"
[ -n "$BIN" ] || { echo "no sutra binary: set SUTRA_BIN or put sutra on PATH" >&2; exit 2; }

CHAPTERS=("$@")
if [ ${#CHAPTERS[@]} -eq 0 ]; then
    CHAPTERS=(docs/src/getting-started/*.md)
fi

WORK="$(mktemp -d /tmp/sutra-doccheck.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
APP=demo                       # the name the docs use for the scaffolded workspace
pass=0; fail=0; skip=0
declare -a FAILED=()

printf '%s\n' "sutra: $BIN ($("$BIN" --version 2>&1 | head -1))"
printf '%s\n\n' "scratch: $WORK"

# Extract the shell lines of a chapter's fenced blocks, in document order. Line-based on
# purpose: a regex over ```…``` mis-pairs fences as soon as the page also contains a mermaid
# block, which silently yielded ONE command out of a dozen the first time this ran.
extract() {
    python3 - "$1" <<'PYEOF'
import re, sys

lines = open(sys.argv[1]).read().splitlines()
shell_langs = {"", "bash", "sh", "shell", "console"}
inside, lang, buf, out = False, "", "", []
for raw in lines:
    if raw.startswith("```"):
        if not inside:
            inside, lang = True, raw[3:].strip().split(",")[0]
        else:
            inside, lang = False, ""
        continue
    if not inside or lang not in shell_langs:
        continue
    line = raw.strip()
    if line.startswith("$ "):
        line = line[2:]
    if not line or line.startswith("#"):
        continue
    buf += line
    if buf.endswith("\\"):          # a continued command
        buf = buf[:-1] + " "
        continue
    out.append(buf.strip())
    buf = ""

# Keep what a reader would actually type in sequence: the sutra invocations plus the directory
# moves and variable assignments they depend on.
for cmd in out:
    if re.match(r"^(sutra\s|cd\s|export\s|mkdir\s)", cmd) or "&& cd" in cmd:
        print(cmd)
PYEOF
}

for chapter in "${CHAPTERS[@]}"; do
    [ -f "$chapter" ] || continue
    printf '\033[1m== %s\033[0m\n' "${chapter#docs/src/}"
    # One workspace per chapter, and one shell state: `cd my-first-app` in an early block has to
    # still apply three blocks later, exactly as it does for a reader.
    ws="$WORK/$(basename "$chapter" .md)"
    mkdir -p "$ws"
    cwd="$ws"
    while IFS= read -r cmd; do
        [ -n "$cmd" ] || continue
        run="${cmd//my-first-app/$APP}"
        run="${run//<name>/$APP}"

        case "$run" in
            *" deploy "*|*" undeploy "*|*" migrate "*|*crypto*|*"<"*">"*)
                printf '  \033[33mSKIP\033[0m %s\n' "$cmd"; skip=$((skip+1)); continue ;;
            # `self-update` without --check REPLACES THE BINARY. A documentation checker must
            # never mutate the thing it is checking with; --check is read-only and does run.
            *self-update*)
                case "$run" in
                    *--check*) ;;
                    *) printf '  \033[33mSKIP\033[0m %s   (mutates the binary)\n' "$cmd"
                       skip=$((skip+1)); continue ;;
                esac ;;
        esac

        # `cd` changes this checker's state rather than running in a subshell that forgets it.
        case "$run" in
            cd\ *)
                target="${run#cd }"
                if [ -d "$cwd/$target" ]; then cwd="$cwd/$target"
                elif [ -d "$target" ]; then cwd="$target"
                # A missing target usually means the block's non-shell prerequisite was
                # dropped (a `git clone` this checker deliberately does not run), so it is a
                # SKIP: the checker's own gap, not a broken promise in the prose.
                else printf '  \033[33mSKIP\033[0m %s   (prerequisite not run here)\n' "$cmd"
                     skip=$((skip+1)); fi
                continue ;;
        esac

        out="$(cd "$cwd" && timeout 180 sh -c "${run//sutra /\"$BIN\" }" 2>&1)"
        rc=$?
        # A trailing `&& cd <dir>` moves this checker too.
        case "$run" in
            *"&& cd "*) [ $rc -eq 0 ] && cwd="$cwd/${run##*&& cd }" ;;
        esac
        if [ $rc -eq 0 ]; then
            printf '  \033[32mPASS\033[0m %s\n' "$cmd"; pass=$((pass+1))
        else
            printf '  \033[31mFAIL\033[0m %s   (exit %d)\n' "$cmd" "$rc"
            printf '        %s\n' "$(printf '%s' "$out" | tail -2 | tr '\n' '|')"
            fail=$((fail+1)); FAILED+=("$cmd")
        fi
    done < <(extract "$chapter")
done

printf '\n\033[1m== result\033[0m\n  %d passed, %d failed, %d skipped (need a live estate)\n' \
    "$pass" "$fail" "$skip"
if [ "$fail" -gt 0 ]; then
    printf '  a documented command that does not run is a broken promise:\n'
    printf '    %s\n' "${FAILED[@]}"
    exit 1
fi
