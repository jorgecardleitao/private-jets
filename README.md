# Danish private flights
This repository contains a CLI application that generates a text based summary of
private jet's flight information targeted to a Danish audience.

It is supported by an Azure Blob storage container for caching data, thereby
reducing its impact to [https://adsbexchange.com/](https://adsbexchange.com/).

## Risk and impact

This code performs API calls to [https://adsbexchange.com/](https://adsbexchange.com/),
a production website of a company.

**Use critical thinking** when using this code and how it impacts them.

We strongly recommend that if you plan to perform large scale analysis (e.g. in time or aircrafts),
that you reach out via an issue _before_, so that we can work together
to cache all hits to [https://adsbexchange.com/](https://adsbexchange.com/)
on an horizontally scaled remote storage and therefore remove its impact to adsbexchange.com
of future calls.

All data cached is available on Azure blob storage:
* account: `privatejets`
* container: `data`

and has anonymous and public read permissions.

## How to use

This repository contains both a Rust library and a set of [`examples/`](./examples) used
to perform actual calculations. To use one of such examples:

1. Install Rust
2. run `cargo run --example single_day -- --tail-number "OY-GFS" --date "2023-10-20"`
3. open `OY-GFS_2023-10-20_0.md`

Step 2. has an optional argument, `--azure-sas-token`, specifying an Azure storage container SAS
for account `privatejets`, container `data`.
When used, cache is written to the remote container, as opposed to disk.

Finally, setting `--backend disk` ignores the Azure's remote storage altogether and
only uses disk for caching (resulting in higher cache misses and thus more
interactions with ADS-B exchange).

In general:
* Use the default parameters when creating ad-hoc stories
* Use `--azure-sas-token` when improving the database with new data.
* Use `--backend disk` when testing the caching system

As of today, the flag `--azure-sas-token` is only available when the code is executed
from `main`, as writing to the blob storage must be done through a controlled code base
that preserves data integrity.

## Methodology

The methodology used to extract information is available at [`methodology.md`](./methodology.md).

## Generated datasets

### Set of worldwide aicrafts whose primary use is to be a private jet:

* [Data](https://privatejets.blob.core.windows.net/data/database/private_jets/2023/11/06/data.csv)
* [Description](https://privatejets.blob.core.windows.net/data/database/private_jets/2023/11/06/description.md)
