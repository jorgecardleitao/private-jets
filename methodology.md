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

For example, `Dassault Falcon 2000` is advertised as a
private jet on https://www.dassaultfalcon.com/aircraft/overview-of-the-fleet/.

This is stored in [`./src/models.csv`](./src/models.csv).

**NOTE**: not all uses of a model whose primary use is to be a private jet is
private jet. For example, models are sometimes used for emergency services.

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

The aircraft at a given segment between two ADS-B positions is considered grounded (not flying) when any of:
1. both positions are on the ground
2. the time between these positions is > 5m and any of the positions is below 10.000 feet
3. the time between these positions is > 10h

Condition 1. is the normal case where ADS-B signal was received when the aircraft landed. 
Condition 2. is used to mitigate the risk that ADS-B receivers sometimes
do not receive an aircraft's signal when the aircraft is at low altitude.
Condition 3. is used to mitigate situations where the aircraft enters regions
of low ADS-B coverage (e.g. central Africa) while flying and then returns flying
(sometimes days later), which should be intepreted as the aircraft being flying for the whole
time.

Source code is available at [src/legs.rs](./src/legs.rs).

### M-5: Compute emissions of leg in a commercial flight

This is performed automatically by the computer program and consists in using the same
metholodogy as used by myclimate.org, available [here](https://www.myclimate.org/en/information/about-myclimate/downloads/flight-emission-calculator/), to compute the emissions of a commercial
flight in first class.

Details are available in the source code, [src/emissions.rs](./src/emissions.rs).

### M-6: Consumption of private jet

This was performed by a human, and consisted:
* access websites of companies that sell private jets
* extract the consumption in gallons per hour (GPH) of each private jet model
* store it in a table with the jet's model, GPH, source and date of extraction, at [`./src/consumption.csv`](./src/consumption.csv).

### M-7: Emissions of a private jet over a leg

This was performed automatically by the program and consisted in performing the
following calculation:

```
leg emissions [kg CO2e] = 
  consumption [gallon/h]
  x liters / gallons [L/gallon]
  x liters to kg of jet fuel [L/kg]
  x emissions per kg [kg CO2 / kg jet fuel]
  x Radiative Forcing index [kg CO2e / kg CO2]
  x Life-cycle emissions [kg CO2e / kg CO2e]
  x leg time [h]
```

Where:

* `consumption` is obtained via the methodology `M-6` in this document.
* `liters / gallons = 3.78541 [L/gallon]`, as specified in [NIST's guide to SI](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication811e2008.pdf)
* `liters to kg of jet fuel = 0.8 [kg/L]`, as [recommended by ICAO](https://data.icao.int/newDataPlus/content/docs/glossary.pdf)
* `emissions per kg = 3.16 [kg CO2 / kg jet fuel]`, as used on [ICAO Carbon Emissions Calculator Methodology, v12 from Sep. 2023](https://applications.icao.int/icec/Methodology%20ICAO%20Carbon%20Calculator_v12-2023.pdf)
* `Radiative Forcing index = 3 [kg CO2e / kg CO2]`, as concluded in [The contribution of global aviation to anthropogenic climate forcing for 2000 to 2018](https://www.sciencedirect.com/science/article/pii/S1352231020305689), from 2021.
* `Life-cycle emissions = 1.68 [kg CO2e / kg CO2e]`, [Life Cycle Greenhouse Gas Emissions from Alternative Jet Fuels v1.2](https://web.mit.edu/aeroastro/partner/reports/proj28/partner-proj28-2010-001.pdf) from 2010-06, accessed 2024-01-28.
* `leg time [h]` is obtained by computing duration of the leg, as identified via the methodology `M-4` in this document.

#### Per passager

```
leg emissions/person [kg CO2e/person] =
  leg emissions [kg CO2e]
  x occupancy [1/person]
```

where
* `leg emissions [kg CO2e]` is as computed above
* `occupancy = 0.23 [1/person] = 1/4.3 [1/person]` obtained from [Average number of passengers per flight who flew private worldwide from 2016 to 2019](https://www.statista.com/statistics/1171518/private-jet-per-flight/), where there were 4.3 passagers per flight in 2019, accessed 2024-01-28.

### M-8: Identify aircraft owner in Denmark

This was performed by a human, and consisted in extracting the ownership of the active
tail number from website https://www.danishaircraft.dk.

For example `OY-CKK` results in 3 records, whose most recent, `OY-CKK(3)`, is registered
to owned by `Kirkbi Invest A/S`.

This is stored in [`./src/owners.csv`](./src/owners.csv).

It also consisted in extracting statements or slogans from these owners from their websites
to illustrate the incompatibility between owning a private jet and their sustainability goals.

This is stored in [`./src/owners.json`](./src/owners.json).
