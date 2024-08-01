# Sia NFS Gateway (sia-nfs)

## Description

`sia-nfs` provides access to one or more Sia buckets via a plain NFS interface, allowing any compatible client to access
the hosted files and directories directly without needing to run a Sia client
or [renterd](https://sia.tech/software/renterd). This gateway can be used locally to mount a Sia filesystem or made
available to an entire network. It is designed to work on Linux, macOS, Windows, and potentially other operating
systems.

## Features

- **NFS v3 support**
- **Cross-Platform:** Runs on every platform where [renterd](https://sia.tech/software/renterd) is available.
- **Integrated Metadata Cache:** Improves access performance, reduces latency, and lowers usage costs.
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

## Status

**WIP!** This is still heavily worked on and not intended to be used at this point.
**Do not use it yet!** It could **EAT YOUR DATA!** Once it is in a more usable state, this message will be updated
accordingly.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

## Acknowledgements

This project has been made possible by the [Sia Foundation's Grant program](https://sia.tech/grants). 
