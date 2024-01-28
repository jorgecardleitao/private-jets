# Methodology

This document describes the general methodology used by this solution.

## Assumptions

* Aircrafts are uniquely identified by a tail number (aka registration number), e.g.
  `OY-EUR`, by the owner of the aircraft.
* Civil aviation in most of the world is mandated to have an ADS-B transponder turned on in-flight.
* Every aircraft flying has a unique transponder identifier (hereby denoted the ICAO number),
  e.g. `4596B2`.
* At any given point in time, there is a one-to-one relationship between the ICAO number and a tail number (`OY-EUR -> 4596B2`)

## Design

* Information can only be obtained from trustworthy publicly available sources that can
be easily verified.
* Statements must be referenced against either existing sources or this methodology.

## Methodology

The methodology used to support this solution is the follow:

### M-1: Identify all aircrafts, ICAO number tail number and type

This is performed automatically by the solution and consists
in extracting the database of all aircrafts in https://globe.adsbexchange.com.

Details are available in the source code, [src/aircraft_db.rs](./src/aircraft_db.rs).

### M-2: Identify aircraft types whose primary use is to be a private flying

This was performed by a human, and consisted in going through different aircraft
manufacturers' websites and identifying the aircrafts that were advertised as used
for private flying.

For example, `Dassault Falcon 2000` (`F2TH` in https://www.icao.int) is advertised as a
private jet on https://www.dassaultfalcon.com/aircraft/overview-of-the-fleet/.

This is stored in [`./src/types.csv`](./src/types.csv).

**NOTE**: not all uses of a model whose primary use is to be a private jet is
private jet. For example, private jets are often used for emergency services.

### M-3: Identify ICAO number's route in a day

This is performed automatically by the computer program and consists in looking for
the historical route of the ICAO number in https://globe.adsbexchange.com.
This contains the sequence of `(latitude, longitude)` and other information.

Each position is assigned the state `Grounded` when
the transponder returns "grounded", else it is assigned the state `Flying`.

Source code is available at [src/icao_to_trace.rs](./src/icao_to_trace.rs).

### M-4: Identify legs of a route

This is performed automatically by the computer program. A leg is defined in this methodology
has a continuous sequence of ADS-B positions in time where the aircraft is flying.

The aircraft at a given segment between two ADS-B positions is considered grounded (not flying) when either:
* both positions are on the ground
* the time between these positions is > 5m and the aircraft is below 10.000 feet

The latter condition is used to mitigate the risk that ADS-B receivers sometimes
do not receive an aircraft's signal when the aircraft is at low altitude.
When this happens for more than 5m, we consider that the aircraft approached and landed.

Source code is available at [src/legs.rs](./src/legs.rs).

### M-5: Compute emissions of leg in a commercial flight

This is performed automatically by the computer program and consists in using the same
metholodogy as used by myclimate.org, available [here](https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/), to compute the emissions of a commercial
flight in first class.

Details are available in the source code, [src/emissions.rs](./src/emissions.rs).

### M-6: Identify aircraft owner in Denmark

This was performed by a human, and consisted in extracting the ownership of the active
tail number from website https://www.danishaircraft.dk.

For example `OY-CKK` results in 3 records, whose most recent, `OY-CKK(3)`, is registered
to owned by `Kirkbi Invest A/S`.

This is stored in [`./src/owners.csv`](./src/owners.csv).

It also consisted in extracting statements or slogans from these owners from their websites
to illustrate the incompatibility between owning a private jet and their sustainability goals.

This is stored in [`./src/owners.json`](./src/owners.json).
