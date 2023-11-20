# {event.owner.claim.name} private jet trip on {event.date}

{event.owner.claim.name} {event.owner.claim.statement.claim}[^1].

Yet, on {event.date}, {event.owner.claim.name} flew one of its private
jets[^2] from {event.from_airport} to {event.to_airport}{{if event.two_way }} and back{{endif}}[^3].

This trip would have emitted {event.commercial_emissions_kg.claim} kg of CO2 in a commercial
flight in first class[^4].
Instead, it emitted around {event.emissions_kg.claim} kg of CO2[^5].
In comparison, a Dane emits {dane_emissions_kg.claim} of CO2 per year[^6].

> ## This single {{if event.two_way }}two{{else}}one{{endif}}-way trip by {event.owner.claim.name} emitted the same as the average Dane emits in {dane_years} years

Billionaires and companies alike are incapable of regulating their emissions,
recklessly destroying the common good.
Ban private jets now and until they emit what equivalent means of transportation would emit.

[^1]: {event.owner.claim.statement.source} - retrieved on {event.owner.claim.statement.date}
[^2]: {event.owner.source} - retrieved on {event.owner.date}
[^3]: {event.source} - retrieved on {event.source_date}
[^4]: {event.commercial_emissions_kg.source} - retrieved on {event.commercial_emissions_kg.date}
[^5]: {event.emissions_kg.source} - retrieved on {event.emissions_kg.date}
[^6]: {dane_emissions_kg.source} - retrieved on {dane_emissions_kg.date}

Copyright Jorge Leitão, released under [CC0](https://creativecommons.org/public-domain/cc0/) - No Rights Reserved.
