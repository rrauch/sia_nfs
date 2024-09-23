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

**BETA** The project has progressed well and is now in a *mostly* usable state.
That being said, this is still a fairly early version with some limitation, caveats &
certainly some bugs.
At this point it is **not recommend** to use `sia_nfs` with important data. 
The server has been tested on Linux (x86_64). Clients have been tested on Linux, macOS & Windows.

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
mkdir demo
mount -t nfs -o nolock,vers=3,tcp,port=12000,mountport=12000,soft [host]:/ demo/
```

#### On macOS:

```bash
mkdir demo
mount_nfs -o nolocks,vers=3,tcp,port=12000,mountport=12000 [host]:/ demo
```

#### On Windows:

**Note:** Windows `Pro` is required, as `Home` does not include an NFS client. Additionally, the NFS client is not
installed by default, so you may need to add it manually.

```bash
mount.exe -o anon,nolock \\[host]\\ X:
```

**Another Note:** Windows expects the NFS mount port to be `111` and does not allow you to specify a different one. You
need to run the Docker container with this command:

```bash
docker run -it --rm -p 111:111 -v sia_nfs_data:/data sia_nfs -e [renterd_api_endpoint] -s [renterd_api_password] -l 0.0.0.0:111 [bucket..]
```

**Third Note:** If you encounter the error `failed to bind port 0.0.0.0:111/tcp`, check if `rpcbind.service`
and/or `rpcbind.socket` are running on your system. Ensure both are stopped and that nothing is listening on port `111`
before trying again.

## Known Issues & Limitations

- As mentioned above, file content can **NOT** be modified. Files can be renamed, moved around etc. but writing to / overwriting existing files is not possible.
- On Windows: currently renaming files and directories can lead to an `Invalid Device` error from Explorer. This seems to be a known issue with NFS & Windows.
- On macOS: Copying files to a `sia_nfs` folder with Finder leads to a "File already exists" error. Copying files via CLI works fine, so this can be used as a workaround for now.
- On macOS: Mounting attempts seem to lead to an infinite loop from the client if `sia_nfs` runs on port `111`. Other ports are fine.


## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

## Acknowledgements

This project has been made possible by the [Sia Foundation's Grant program](https://sia.tech/grants). 
