#!/usr/bin/env bash
# Installs or upgrades hyrax-bask as a systemd user service for the current user.
# Run on the target machine: install-agent.sh <wheel> <env-file> <unit-file> <ca-file>
# The env file sets HYRAX_SERVER, HYRAX_NODE and HYRAX_AGENT_TOKEN (from `hyrax admin enroll <node>`).
# Pass - as the env file to upgrade an agent and keep the settings and token it has.
# The CA file is the control plane's ca.pem; agents and the CLI trust nothing else.
set -euo pipefail

wheel=$1
env_file=$2
unit_file=$3
ca_file=$4
home_dir=$HOME/.local/share/hyrax

# uv rather than `python3 -m venv`, which needs the python3-venv apt package.
# Machines without uv get a copy of the binary at $home_dir/uv.
uv=$(command -v uv || echo "$home_dir/uv")
if [ ! -x "$uv" ]; then
    echo "uv not found: install it or copy the binary to $home_dir/uv" >&2
    exit 1
fi
rm -rf "$home_dir/venv"
"$uv" venv --quiet --python "$(command -v python3)" "$home_dir/venv"
"$uv" pip install --quiet --python "$home_dir/venv/bin/python" "$wheel"

mkdir -p "$HOME/.config/hyrax" "$HOME/.config/systemd/user"
if [ "$env_file" != "-" ]; then
    install -m 600 "$env_file" "$HOME/.config/hyrax/agent.env"
fi
install -m 644 "$ca_file" "$HOME/.config/hyrax/ca.pem"
grep -q "^HYRAX_CA=" "$HOME/.config/hyrax/agent.env" || echo "HYRAX_CA=$HOME/.config/hyrax/ca.pem" >> "$HOME/.config/hyrax/agent.env"
# The CLI on this machine, same package.
mkdir -p "$HOME/.local/bin"
ln -sf "$home_dir/venv/bin/hyrax" "$HOME/.local/bin/hyrax"
install -m 644 "$unit_file" "$HOME/.config/systemd/user/hyrax-bask.service"

systemctl --user daemon-reload
systemctl --user enable --quiet hyrax-bask
systemctl --user restart hyrax-bask
