#!/bin/sh
# trexsql container entrypoint.
#
# Responsibilities:
#   1. Ensure a TLS keypair exists at /usr/src/server.{crt,key}.
#      If missing, generate a per-container self-signed cert.
#   2. Replace any known-default placeholder values for sensitive
#      environment variables (BETTER_AUTH_SECRET, PGRST_JWT_SECRET)
#      with strong randomly-generated secrets.
#   3. If TREX_PRODUCTION_MODE=1, refuse to start when any default
#      placeholder value is still in effect.
#   4. exec the trex binary, forwarding any CLI arguments passed
#      to docker run / compose command.
#
# Note: the self-signed certificate generated here is per-container
# and NOT suitable for production use. Real deployments MUST mount
# their own certificate at /usr/src/server.crt and matching key at
# /usr/src/server.key (or provide them via a secrets manager).

set -eu

CRT=/usr/src/server.crt
KEY=/usr/src/server.key

# Known-default placeholder secrets. Anything matching these is treated
# as "operator forgot to override" and replaced (or rejected in prod).
DEFAULT_SECRET_PLACEHOLDERS="dev-secret-at-least-32-characters-long!! dev-secret changeme please-change-me"

is_default_placeholder() {
    val="${1:-}"
    if [ -z "$val" ]; then
        return 0
    fi
    for placeholder in $DEFAULT_SECRET_PLACEHOLDERS; do
        if [ "$val" = "$placeholder" ]; then
            return 0
        fi
    done
    return 1
}

generate_secret() {
    # 48 bytes -> 64 base64 chars, well above the 32-char minimum
    openssl rand -base64 48 | tr -d '\n='
}

# ---------------------------------------------------------------------------
# 1. TLS certificate
# ---------------------------------------------------------------------------
if [ -f "$CRT" ] && [ -f "$KEY" ]; then
    echo "STARTUP: using mounted TLS cert at $CRT"
else
    echo "STARTUP: generating per-container self-signed TLS cert at $CRT"
    echo "STARTUP: this cert is NOT suitable for production. Mount /usr/src/server.crt and /usr/src/server.key for real deployments."
    # -newkey rsa:2048 forces a fresh key; -nodes leaves it unencrypted (the
    # process needs to read it on its own); subjectAltName covers localhost
    # and 127.0.0.1 so modern clients accept it.
    openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
        -keyout "$KEY" -out "$CRT" \
        -subj "/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
        >/dev/null 2>&1
    chmod 644 "$CRT"
    chmod 600 "$KEY"
fi

# ---------------------------------------------------------------------------
# 2. Secrets: BETTER_AUTH_SECRET and PGRST_JWT_SECRET
# ---------------------------------------------------------------------------
PRODUCTION_MODE="${TREX_PRODUCTION_MODE:-0}"

check_or_replace_secret() {
    name="$1"
    current_val="$(eval "printf '%s' \"\${$name:-}\"")"
    if is_default_placeholder "$current_val"; then
        if [ "$PRODUCTION_MODE" = "1" ]; then
            echo "STARTUP: FATAL: TREX_PRODUCTION_MODE=1 but $name is unset or matches a known-default placeholder." >&2
            echo "STARTUP: Refusing to start. Set $name to a strong unique value." >&2
            exit 1
        fi
        new_val="$(generate_secret)"
        export "$name=$new_val"
        echo "==============================================================================="
        echo "STARTUP: generated random $name=$new_val"
        echo "STARTUP: Set this in your compose env to persist across restarts."
        echo "==============================================================================="
    fi
}

check_or_replace_secret BETTER_AUTH_SECRET
check_or_replace_secret PGRST_JWT_SECRET

# ---------------------------------------------------------------------------
# 3. exec trex
# ---------------------------------------------------------------------------
exec trex "$@"
