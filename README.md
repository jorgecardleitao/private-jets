# Danish private flights
This repository contains a small application that generates a text based summary of
private jet's flight information targetted to a Danish audience.

## How to use

1. Install Rust
2. Go to https://globe.adsbexchange.com/?icao=459053&showTrace=2023-10-13 and fetch the cookies
   used to retrieve the file `trace_full_459563.json` (once per day)
3. run `cargo run -- --tail-number "OY-GFS" --date "2023-10-20" --cookie "adsbx_sid=1697996994839_e9zejgp1o; adsbx_api=1697997662491_tl8d1cpxfvi"` with the cookie
   replaced by what you fetched in step 2.

We hope to remove step 2 soon.

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

#### 2. Identify private jet aircrafts

This is performed automatically by the computer program and consists, and
consisted in extracting the database of all aicrafts in https://globe.adsbexchange.com.

For example, `OY-CKK` had ICAO number `458D6B` as of 2023-11-06.

Details are available in the source code, [src/number_of_icao.rs](./src/number_of_icao.rs).

For example, advance searching for aircraft type `Dassault Falcon 2000` in
https://www.danishaircraft.dk yields `OY-CKK(3)`. To corruburate that this is
an active aicraft, searching for `OY-CKK` in https://globe.adsbexchange.com results in a match.

This is stored in [`./src/aircrafts.csv`](./src/aircrafts.csv).

#### 3. Identify aircraft owner in denmark

This was performed by a human, and consisted in extracting the ownership of the active
tail number from website https://www.danishaircraft.dk.

For example `OY-CKK` results in 3 records, whose most recent, `OY-CKK(3)`, is registered
to owned by `Kirkbi Invest A/S`.

This is stored in [`./src/aircrafts.csv`](./src/aircrafts.csv).

It also consisted in extracting statements or slogans from these owners from their websites
to illustrate the incompatibility between owning a private jet and their sustainability goals.

This is stored in [`./src/owners.json`](./src/owners.json).

#### 4. Identify transponder ICAO number from tail number

This is performed automatically by the computer program and consists
in looking the tail number in the same database used by https://globe.adsbexchange.com.

For example, `OY-CKK` had ICAO number `458d6b` as of 2023-10-23.

Details are available in the source code, [src/number_of_icao.rs](./src/number_of_icao.rs).

#### 5. Identify ICAO number's route in a day

This is performed automatically by the computer program and consists in looking for
the historical route of the ICAO number in https://globe.adsbexchange.com.
This contains the sequence of `(latitude, longitude)` and other information.

Details are available in the source code, [src/legs.rs](./src/legs.rs).

#### 6. Identify legs of a route

This is performed automatically by the computer program and consists in identifying
points during the flight that the aircraft is in mode "ground", and computing the leg
between two ground situations.

Since some aircrafts only turn on the transponder while in flight, we set that below 1000 feet
the aircraft is considered on the ground.

Details are available in the source code, [src/legs.rs](./src/legs.rs).

#### 7. Compute emissions of leg

This is performed automatically by the computer program and consists in using the same
metholodogy as used by myclimate.org, available [here](https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/), to compute the emissions of a commercial
flight in first class.

Details are available in the source code, [src/emissions.rs](./src/emissions.rs).

#### 8. Writing output

This is performed automatically by the computer program and consists in a template, available
in [`src/template.md`](./src/template.md), to produce a complete document.

Details are available in the source code, [src/main.rs](./src/main.rs).

## Design

* Information can only be obtained from trustworthy publicly available sources that can
be easily verified.
* Main statements must be referenced against these sources
