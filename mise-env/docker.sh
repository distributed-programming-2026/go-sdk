#!/bin/sh

# Docker CLI resolves the active context itself, while Testcontainers reads
# DOCKER_HOST directly. Export the context endpoint when the caller did not
# provide an explicit Docker host.
if [ -z "${DOCKER_HOST:-}" ] && command -v docker >/dev/null 2>&1; then
	docker_context_host="$(docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)"
	if [ -n "$docker_context_host" ]; then
		export DOCKER_HOST="$docker_context_host"
	fi
fi

# For VM-backed Docker contexts (Lima, Colima, Docker Desktop), Ryuk must mount
# the socket path as it exists inside the VM, not the macOS host path.
case "${DOCKER_HOST:-}" in
	unix://*/.lima/*|unix://*/.colima/*|unix://*/.docker/run/*)
		: "${TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE:=/var/run/docker.sock}"
		export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE
		;;
esac

unset docker_context_host
