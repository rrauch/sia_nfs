# Sia NFS Gateway (sia_nfs)

## Description

`sia_nfs` provides access to one or more Sia buckets via a plain NFS interface, allowing any compatible client to access
the hosted files and directories directly without needing to run a Sia client
or [renterd](https://sia.tech/software/renterd). This gateway can be used locally to mount a Sia filesystem or made
available to an entire network. It is designed to work on Linux, macOS, Windows, and potentially other operating
systems.

## Features

- **NFS v3 support**
- **Cross-Platform:** Runs on every platform where [renterd](https://sia.tech/software/renterd) is available.
- **Integrated Metadata & Content Cache:** Improves access performance, reduces latency, and lowers usage costs.
- **Open Source:** Released under the Apache-2.0 and MIT licenses.

## Functionality

- **Multi-Bucket Support:** One or more buckets can be made available for a unified file system.
- **Mounting and Unmounting:** Mounting and unmounting works as expected.
- **Directory Listing:** Files and directories can be listed and navigated.
- **Directory Creation:** New empty directories can be created.
- **Directory Removal:** Empty directories can be deleted.
- **File Deletion:** Files can be deleted.
- **Renaming and Moving:** Files and directories can be renamed and moved (within the same bucket).
- **File Reading:** Files can be read as expected, including seeking.
- **File Writing:** New files can be created and written to, including copying an existing file.

**Note:** Existing files cannot be written to as Sia objects are immutable.

## Status

**BETA**: The project has progressed well and is now in a fairly usable state. However, this version is not yet
considered mature and comes with some limitations, caveats, and certainly some bugs. At this point, it is **not
recommended** to use `sia_nfs` with important data.

The server has been tested on Linux (x86_64). Clients have been tested on Linux, macOS, and Windows.

*Use at your own risk.*

## Test Drive

A `Dockerfile` as well as a pre-built image is available to make it easy for you to try out `sia_nfs`.

### Build from Source

```bash
# Clone the repository
git clone https://github.com/rrauch/sia_nfs.git
cd sia_nfs
# Build the Docker image
docker build ./ -t sia_nfs
```

or

### Pull from Github Container Registry

```bash
docker pull ghcr.io/rrauch/sia_nfs:latest
docker tag ghcr.io/rrauch/sia_nfs:latest sia_nfs:latest
```

### Run

```bash
# Create a persistent volume `sia_nfs_data`
docker volume create sia_nfs_data

# Run the Docker container in the foreground
docker run -it --rm -p 12000:12000 -v sia_nfs_data:/data sia_nfs -e [renterd_api_endpoint] -s [renterd_api_password] [bucket..]
```

Replace `[renterd_api_endpoint]` with the URL of your renterd API, e.g., `http://localhost:9880/api/`.
Replace `[renterd_api_password]` with your API password. Finally, replace `[bucket..]` with the names of one or more
buckets you want
to export.

You can now connect from any NFSv3-compatible client. Note that both the portmapper port and the mount port are set to
`12000`.

### Mount

Replace `[host]` with the address of the machine where the Docker container is running, e.g., `localhost`.

#### On Linux (`sudo` may be required):

```bash
mkdir sia
mount -t nfs -o nolock,vers=3,tcp,port=12000,mountport=12000,soft [host]:/sia sia
```

#### On macOS:

```bash
mkdir sia
mount_nfs -o nolocks,vers=3,tcp,port=12000,mountport=12000 [host]:/sia sia
```

#### On Windows:

**Note:** Windows `Pro` is required, as `Home` does not include an NFS client. Additionally, the NFS client is not
installed by default, so you may need to add it manually.

```bash
mount.exe -o anon,nolock \\[host]\sia S:
```

**Another Note:** Windows expects the NFS mount port to be `111` and does not allow you to specify a different one. You
need to run the Docker container with this command:

```bash
docker run -it --rm -p 111:111 -v sia_nfs_data:/data sia_nfs -e [renterd_api_endpoint] -s [renterd_api_password] -l 0.0.0.0:111 [bucket..]
```

**Third Note:** If you encounter the error `failed to bind port 0.0.0.0:111/tcp`, check if `rpcbind.service`
and/or `rpcbind.socket` are running on your system. Ensure both are stopped and that nothing is listening on port `111`
before trying again.

## Caching

`sia_nfs` caches both metadata and content. *Metadata* caching is always active and automatically syncs with `renterd`,
either periodically or whenever a file system change is made via `sia_nfs`.

*Content* is cached in deduplicated chunks and is always checked against `renterd` to ensure no stale content is
delivered. By default, the disk cache is limited to a maximum size of `2 GiB`. This can be configured using
the `--max-cache-size` argument. Setting this value to `0` completely disables the disk cache. The location of the disk
cache can also be configured via the `--cache-dir` argument and can be set separately from the `--data-dir`, which
contains `sia_nfs`'s main persistent data, such as the metadata cache.

**Please note:** Changes made directly to `renterd` (not via `sia_nfs`) will **NOT** be reflected immediately, as there
is currently no mechanism to receive notifications of changes via the `renterd` API.

## Known Issues & Limitations

- As mentioned above, file content can **NOT** be modified. Files can be renamed, moved around etc. but writing to /
  overwriting existing files is not possible.
- On Windows: The Windows client requires `sia_nfs` to run on port 111. Other ports are not supported.
- On macOS: Mounting attempts seem to lead to an infinite loop from the client if `sia_nfs` runs on port `111`. Other
  ports are fine.
- Write errors: Due to the stateless nature of NFSv3, `sia_nfs` cannot clearly determine when a client is done writing
  to a file, as there is no `close` call or equivalent. A file is considered closed if there are no more write calls for
  a specific amount of time, with the default being `10s`. In rare cases, this can lead to `sia_nfs` closing a file
  prematurely if the client pauses writing but hasn't actually finished. If you encounter this issue, you can adjust the
  timeout period using the `--write-autocommit-after` argument. For example, you can set it to `20s`, `30s` or
  even `1min` to better suit your specific situation.

## Usage

```bash
:~$ sia_nfs --help
Exports Sia buckets via NFS. Connects to renterd, allowing direct NFS access to exported buckets

Usage: sia_nfs [OPTIONS] --renterd-api-endpoint <RENTERD_API_ENDPOINT> --renterd-api-password <RENTERD_API_PASSWORD> --data-dir <DATA_DIR> <BUCKETS>...

Arguments:
  <BUCKETS>...  List of buckets to export

Options:
  -e, --renterd-api-endpoint <RENTERD_API_ENDPOINT>
          URL for renterd's API endpoint (e.g., http://localhost:9880/api/) [env: RENTERD_API_ENDPOINT=]
  -s, --renterd-api-password <RENTERD_API_PASSWORD>
          Password for the renterd API. It's recommended to use an environment variable for this [env: RENTERD_API_PASSWORD=]
  -d, --data-dir <DATA_DIR>
          Directory to store persistent data in. Will be created if it doesn't exist [env: DATA_DIR=/data/]
  -c, --cache-dir <CACHE_DIR>
          Optional directory to store the content cache in. Defaults to `DATA_DIR` if not set. Will be created if it doesn't exist [env: CACHE_DIR=]
  -m, --max-cache-size <MAX_CACHE_SIZE>
          Maximum size of content cache. Set to `0` to disable [env: MAX_CACHE_SIZE=] [default: "2 GiB"]
  -l, --listen-address <LISTEN_ADDRESS>
          Host and port to listen on [env: LISTEN_ADDRESS=0.0.0.0:12000] [default: localhost:12000]
      --uid <UID>
          UID of files and directories [env: INODE_UID=] [default: 1000]
      --gid <GID>
          GID of files and directories [env: INODE_GID=] [default: 1000]
      --file-mode <FILE_MODE>
          Unix file permissions [env: FILE_MODE=] [default: 0600]
      --dir-mode <DIR_MODE>
          Unix directory permissions [env: DIR_MODE=] [default: 0700]
      --write-autocommit-after <WRITE_AUTOCOMMIT_AFTER>
          Time without write activity after which a new file is considered complete [env: WRITE_AUTOCOMMIT_AFTER=] [default: 10s]
  -h, --help
          Print help
  -V, --version
          Print version
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

## Acknowledgements

This project has been made possible by the [Sia Foundation's Grant program](https://sia.tech/grants). 
