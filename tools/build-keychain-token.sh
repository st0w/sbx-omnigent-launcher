#!/bin/sh
# Build, sign and install tools/keychain-token (see its header for WHY).
#
#   sh tools/build-keychain-token.sh [signing-identity]
#
# The identity may also come from CODESIGN_IDENTITY; with neither, the
# first codesigning identity on this host is used. The install path is
# ~/.local/libexec/keychain-token, overridable with KEYCHAIN_TOKEN_DEST.
#
# The identity matters more than it looks. A keychain ACL records the
# binary's DESIGNATED REQUIREMENT, and a real signing identity yields
# one built from the identifier plus the certificate:
#
#   identifier "com.example.token" and anchor apple generic and
#   certificate leaf[subject.CN] = "Apple Development: …"
#
# which keeps matching across rebuilds. Ad-hoc signing (`-`) yields a
# cdhash requirement instead, so every recompile silently breaks the
# grant and the next publish starts prompting. This refuses ad-hoc for
# that reason.

set -eu

SRC="$(dirname "$0")/keychain-token.c"
DEST="${KEYCHAIN_TOKEN_DEST:-$HOME/.local/libexec/keychain-token}"
IDENT="${1:-${CODESIGN_IDENTITY:-}}"

[ -f "$SRC" ] || { echo "no source at $SRC" >&2; exit 1; }

if [ -z "$IDENT" ]; then
    IDENT=$(security find-identity -v -p codesigning 2>/dev/null |
            sed -n 's/^ *[0-9]*) *[0-9A-F]* *"\(.*\)"$/\1/p' | head -1)
fi
[ -n "$IDENT" ] || {
    echo "no codesigning identity found. Pass one explicitly:" >&2
    echo "  security find-identity -v -p codesigning" >&2
    echo "  sh $0 'Apple Development: You (TEAMID)'" >&2
    exit 1
}
[ "$IDENT" = "-" ] && {
    echo "refusing to ad-hoc sign: the keychain grant would break on" >&2
    echo "every rebuild. Use a real identity (see the header)." >&2
    exit 1
}

mkdir -p "$(dirname "$DEST")"
clang -O2 -Wall -Werror -o "$DEST" "$SRC" \
    -framework Security -framework CoreFoundation
codesign -f -s "$IDENT" -i com.example.token "$DEST"

# 0700, not the 0755 clang leaves behind. Anyone who can EXECUTE this
# binary reads the token with no prompt — that is the whole point of it
# — so execute permission is the last boundary left. Owner only.
chmod 0700 "$DEST"

echo "installed: $DEST"
ls -l "$DEST" | sed 's/^/  /'
echo
echo "designated requirement now recorded by any keychain grant:"
codesign -d -r- "$DEST" 2>&1 | sed -n 's/^designated => /  /p'
echo
echo "Grant it access to an item (one time, per item):"
echo "  security delete-generic-password -s <service>"
echo "  security add-generic-password -a <account> -s <service> \\"
echo "      -T \"\" -T \"$DEST\" -w"
