#!/usr/bin/env bash

set -euo pipefail

PREFIX="/usr/local"
WORKDIR="$(pwd)/.perf_build"

log() { echo -e "[install] $*" >&2; }
err() { echo -e "[install][ERROR] $*" >&2; }

INSTALL_BIN_DIR="$PREFIX/bin"

detect_pkg_mgr() {
	if command -v apt-get >/dev/null 2>&1; then
		echo apt
	else
		echo unknown
	fi
}

is_wsl() {
	if grep -qiE 'microsoft|wsl' /proc/version 2>/dev/null; then
		return 0
	fi
	if [ -n "${WSL_DISTRO_NAME:-}" ]; then
		return 0
	fi
	return 1
}

require_cmd() {
	local c
	for c in "$@"; do
		if ! command -v "$c" >/dev/null 2>&1; then
			err "Required command not found: $c"; exit 1
		fi
	done
}

install_base_tools() {
	local pm
	pm=$(detect_pkg_mgr)
	if [[ "$pm" == apt ]]; then
		log "Installing strace and valgrind…"
		sudo apt-get update -y
		sudo apt-get install -y strace valgrind
	else
		err "Unsupported distribution (no apt-get). Please install strace and valgrind manually."
	fi
}

verify_perf_functional() {
	if ! command -v perf >/dev/null 2>&1; then
		return 1
	fi
	if ! perf --version >/dev/null 2>&1; then
		return 1
	fi
	# Try a tiny perf stat; if restricted, still consider installed OK
	perf stat -e task-clock /bin/true >/dev/null 2>&1 || true
	return 0
}

# ------------------- Upstream (non-WSL) build path -------------------
install_deps_full_upstream() {
	# Reuse the same full-feature dependency set as WSL build
	install_deps_full_wsl
}

clone_linux_upstream() {
	local repo dest series kv tag
	repo="https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git"
	dest="$WORKDIR/linux-stable"
	# Determine running kernel numeric (e.g., 6.14.0-33-generic -> 6.14)
	kv=$(uname -r | cut -d'-' -f1)
	series=$(echo "$kv" | awk -F. '{print $1"."$2}')
	# Query remote tags matching this series and pick the newest (semantic sort)
	log "Selecting upstream stable tag for series v${series}.x …"
	tag=$(git ls-remote --tags --refs "$repo" "v${series}.*" | awk '{print $2}' | sed 's#refs/tags/##' | sort -V | tail -n1)
	if [[ -z "$tag" ]]; then
		err "Unable to find upstream tag for series v${series}.x. Falling back to default branch."
		tag=""
	else
		log "Chosen upstream tag: $tag"
	fi
	if [[ -d "$dest/.git" ]]; then
		log "linux-stable already present: $dest"; echo "$dest"; return 0
	fi
	if [[ -n "$tag" ]]; then
		log "Cloning linux-stable tag $tag (depth=1)…"
		git clone --depth 1 --branch "$tag" "$repo" "$dest"
	else
		log "Cloning linux-stable default branch (depth=1)…"
		git clone --depth 1 "$repo" "$dest"
	fi
	echo "$dest"
}

build_perf_upstream() {
	local src
	src="$1"
	log "Building tools/perf from upstream …"
	PYTHONWARNINGS=ignore::SyntaxWarning \
		make -C "$src/tools/perf" -j"$(nproc)" WERROR=0
}

# ------------------- WSL build path -------------------
install_deps_full_wsl() {
	local pm
	pm=$(detect_pkg_mgr)
	if [[ "$pm" == apt ]]; then
		log "Installing full-feature dependencies for perf (WSL)…"
		sudo apt-get update -y
		sudo apt-get install -y \
			bc build-essential flex bison dwarves git make pkg-config \
			libssl-dev libelf-dev \
			binutils-dev debuginfod default-jdk default-jre \
			libaio-dev libbabeltrace-dev libcap-dev libdw-dev libdwarf-dev \
			libiberty-dev liblzma-dev libnuma-dev libperl-dev libpfm4-dev \
			libslang2-dev libtraceevent-dev libunwind-dev libzstd-dev libzstd1 \
			libcapstone-dev libbpf-dev libcurl4-openssl-dev \
			python3 python3-dev python3-setuptools systemtap-sdt-dev zlib1g-dev
	else
		err "Unsupported distribution; please install the required libraries manually."; exit 1
	fi
}

prep_workdir() { mkdir -p "$WORKDIR"; }

get_kernel_version_wsl() { uname -r | cut -d'-' -f1; }

clone_wsl2_kernel() {
	local kv tag repo dest
	kv="$1"; tag="linux-msft-wsl-${kv}"; repo="https://github.com/microsoft/WSL2-Linux-Kernel.git"
	dest="$WORKDIR/WSL2-Linux-Kernel-${kv}"
	if [[ -d "$dest/.git" ]]; then
		log "Kernel source already present: $dest"; echo "$dest"; return 0
	fi
	log "Cloning WSL2 kernel repo at tag $tag …"
	if git clone --depth 1 --single-branch --branch "$tag" "$repo" "$dest"; then
		echo "$dest"; return 0
	fi
	err "Failed to clone tag $tag from $repo. Check releases for a matching tag."; exit 1
}

build_kernel_config() {
	local src
	src="$1"
	log "Preparing kernel configuration…"
	PYTHONWARNINGS=ignore::SyntaxWarning make -C "$src" -j"$(nproc)" KCONFIG_CONFIG=Microsoft/config-wsl >/dev/null
}

build_perf_wsl() {
	local src
	src="$1"
	log "Building tools/perf …"
	PYTHONWARNINGS=ignore::SyntaxWarning \
		make -C "$src/tools/perf" -j"$(nproc)" WERROR=0
}

install_perf_binary() {
	local src
	src="$1"
	require_cmd sudo
	sudo mkdir -p "$INSTALL_BIN_DIR"
	sudo cp -f "$src/tools/perf/perf" "$INSTALL_BIN_DIR/perf"
	log "Installed: $(command -v perf || echo "$INSTALL_BIN_DIR/perf")"
	perf --version || true
}

add_alias() {
	local rc target_alias begin_marker end_marker
	target_alias="$INSTALL_BIN_DIR/perf"
	if [[ ! -x "$target_alias" ]]; then
		log "Alias step: $target_alias not found or not executable; skipping."
		return 0
	fi
	case "${SHELL:-/bin/bash}" in
		*zsh) rc="$HOME/.zshrc" ;;
		*) rc="$HOME/.bashrc" ;;
	esac
	begin_marker="# >>> perf-install alias start >>>"
	end_marker="# <<< perf-install alias end <<<"
	if [[ -f "$rc" ]] && grep -q "$begin_marker" "$rc"; then
		log "Alias block already present in $rc"
	else
		log "Adding alias block to $rc"
		{
			echo "$begin_marker"
			echo "if [ -x '$target_alias' ]; then"
			echo "  alias perf='$target_alias'"
			echo "fi"
			echo "$end_marker"
		} >> "$rc"
		log "To activate now: source $rc"
	fi
}

main() {
	if [[ ${EUID:-$(id -u)} -eq 0 ]]; then
		err "Please do not run this script as root. It will use sudo when needed."; exit 1
	fi

	install_base_tools

	if is_wsl; then
		log "WSL environment detected; building perf from WSL2 kernel source."
		prep_workdir
		install_deps_full_wsl
		local kv srcdir
		kv=$(get_kernel_version_wsl)
		log "Running kernel (numeric): $kv"
		srcdir=$(clone_wsl2_kernel "$kv")
		build_kernel_config "$srcdir"
		build_perf_wsl "$srcdir"
		install_perf_binary "$srcdir"
		add_alias
	else
		log "Non-WSL environment detected; building perf from upstream linux-stable."
		prep_workdir
		install_deps_full_upstream
		local srcdir
		srcdir=$(clone_linux_upstream)
		build_perf_upstream "$srcdir"
		install_perf_binary "$srcdir"
		add_alias
	fi

	log "Done. perf version: $(perf --version 2>/dev/null || echo 'not available')"
	if command -v perf >/dev/null 2>&1; then
		echo "[install] perf build options:" >&2
		perf version --build-options 2>/dev/null || true
	fi

	# Enable unrestricted perf access
	log "Configuring kernel to allow unrestricted perf access…"
	current_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "unknown")
	if [[ "$current_paranoid" != "1" ]]; then
		sudo sysctl kernel.perf_event_paranoid=1
		# Make it permanent across reboots
		if ! grep -q "^kernel.perf_event_paranoid" /etc/sysctl.conf 2>/dev/null; then
			echo "kernel.perf_event_paranoid = 1" | sudo tee -a /etc/sysctl.conf >/dev/null
			log "Added kernel.perf_event_paranoid=1 to /etc/sysctl.conf (permanent)"
		else
			log "kernel.perf_event_paranoid already configured in /etc/sysctl.conf"
		fi
	else
		log "perf_event_paranoid already set to 1 (unrestricted)"
	fi

	# Cleanup build workspace on success
	if [[ -d "$WORKDIR" ]]; then
		log "Cleaning up build workspace: $WORKDIR"
		rm -rf "$WORKDIR"
	fi
}

main "$@"

