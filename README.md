# docker-logql

A simple Docker CLI plugin to run LogQL queries over docker container logs.

## Installation

1. Build `docker-logql` binary.
   - **NOTE**: `docker-` prefix is important, docker would not find plugin without it.
2. Add binary to [plugin directory](https://github.com/docker/cli/blob/34797d167891c11d2e10c1339b072166b77a3378/cli-plugins/manager/manager_unix.go#L5-L8)
   - `~/.docker/cli-plugins` for current user
   - `/usr/local/libexec/docker/cli-plugins` for system-wide installation

Or use `make install`, it would build and add plugin to `~/.docker/cli-plugins` directory.

```console
git clone https://github.com/tdakkota/docker-logql
cd docker-logql
make install
```
