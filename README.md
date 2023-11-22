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

## Assumptions

* Aircrafts are uniquely identified by a tail number (aka registration number), e.g.
  `OY-EUR`, by the owner of the aircraft.
* Civil aviation in Europe is mandated to have an ADS-B transponder turned on in-flight.
* Every aircraft flying has a unique transponder identifier (hereby denoted the ICAO number),
  e.g. `4596B2`.
* At any given point in time, there is a one-to-one relationship between the ICAO number and a tail number (`OY-EUR -> 4596B2`)

## Functional specification

### FS-1 - Behaviour

This solution is a CLI executed in a terminal on Windows, Linux or Mac OS.

It receives two arguments, a tail number and a date, and writes a
markdown file with a description of:
* the owner of said tail number
* the legs that tail number flew on that date
* how many emissions (CO2e) were emitted
* how many emissions (CO2e) would have been emitted if a commercial flight would
  have been taken instead.
* how many emissions per year (CO2e/y) a Dane emits
* The source of each of the claims.

templated based on [`src/template.md`](./src/template.md).

### FS-2 - Methodology

The methodology used to support this solution is the follow:

#### 1. Identify aircraft types whose primary use is private jet flying

This was performed by a human, and consisted in going through different aircraft
manufacturers' websites and identifying the aircrafts that were advertised as used
for private flying.

For example, `Dassault Falcon 2000` (`F2TH` in https://www.icao.int) is advertised as a
private jet on https://www.dassaultfalcon.com/aircraft/overview-of-the-fleet/.

This is stored in [`./src/types.csv`](./src/types.csv).

#### 2. Identify all aircrafts, ICAO number tail number and type

This is performed automatically by the computer program and consists
in extracting the database of all aircrafts in https://globe.adsbexchange.com.

Details are available in the source code, [src/aircraft_db.rs](./src/aircraft_db.rs).

#### 3. Identify aircraft owner in denmark

This was performed by a human, and consisted in extracting the ownership of the active
tail number from website https://www.danishaircraft.dk.

For example `OY-CKK` results in 3 records, whose most recent, `OY-CKK(3)`, is registered
to owned by `Kirkbi Invest A/S`.

This is stored in [`./src/owners.csv`](./src/owners.csv).

It also consisted in extracting statements or slogans from these owners from their websites
to illustrate the incompatibility between owning a private jet and their sustainability goals.

This is stored in [`./src/owners.json`](./src/owners.json).

#### 4. Identify ICAO number's route in a day

This is performed automatically by the computer program and consists in looking for
the historical route of the ICAO number in https://globe.adsbexchange.com.
This contains the sequence of `(latitude, longitude)` and other information.

Details are available in the source code, [src/legs.rs](./src/legs.rs).

#### 5. Identify legs of a route

This is performed automatically by the computer program and consists in identifying
points during the flight that the aircraft is in mode "ground", and computing the leg
between two ground situations.

Since some aircrafts only turn on the transponder while in flight, we set that below 1000 feet
the aircraft is considered on the ground.

Details are available in the source code, [src/legs.rs](./src/legs.rs).

#### 8. Compute emissions of leg

This is performed automatically by the computer program and consists in using the same
metholodogy as used by myclimate.org, available [here](https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/), to compute the emissions of a commercial
flight in first class.

Details are available in the source code, [src/emissions.rs](./src/emissions.rs).

#### 9. Write output

This is performed automatically by the computer program and consists in a template, available
in [`src/template.md`](./src/template.md), to produce a complete document.

Details are available in the source code, [src/main.rs](./src/main.rs).

## Design

* Information can only be obtained from trustworthy publicly available sources that can
be easily verified.
* Main statements must be referenced against these sources
