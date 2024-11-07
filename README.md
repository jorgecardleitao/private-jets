# Private jet flights
[![Test](https://github.com/jorgecardleitao/private-jets/actions/workflows/test.yaml/badge.svg)](https://github.com/jorgecardleitao/private-jets/actions/workflows/test.yaml)
[![Coverage](https://codecov.io/gh/jorgecardleitao/private-jets/graph/badge.svg?token=DT7C376OKH)](https://codecov.io/gh/jorgecardleitao/private-jets)

This repository contains a CLI application to analyze flights of private jets.

It is supported by an S3 Blob storage container for caching data, thereby
reducing its impact to [https://adsbexchange.com/](https://adsbexchange.com/).

It resulted in a scientific publication in [Communications Earth & Environment paper](https://www.nature.com/articles/s43247-024-01775-z),
and a bunch of press about it:

* [Financial Times](https://www.ft.com/content/13a89ac6-6fa8-4e17-9ef2-698d20b657a7)
* [Associated Press](https://apnews.com/article/climate-change-private-jets-wealthy-carbon-pollution-0a2d1d2cd81906381953346bfdb879e8)
* [The Guardian](https://www.theguardian.com/world/2024/nov/07/used-like-taxis-soaring-private-jet-flights-drive-up-climate-heating-emissions)
* [BBC](https://www.bbc.com/news/articles/cx2lvq4el5vo)
* [The times](https://www.thetimes.com/uk/environment/article/celebrity-private-jets-co2-emissions-5gmvgncrl)
* [National Geographic](https://www.nationalgeographic.com/environment/article/private-jet-flights-climate-change)
* [News Scientist](https://www.newscientist.com/article/2455196-carbon-emissions-from-private-jets-have-exploded-in-recent-years/)
* [AOL](https://www.aol.com/ultra-rich-using-jets-taxis-163749747.html?guccounter=1)
* [Lufkin Daily News](https://lufkindailynews.com/anpa/us/carbon-pollution-from-high-flying-rich-in-private-jets-soars/article_ac190bb5-4f01-5d04-8f87-25bd49778d9a.html)
* [AFR](https://www.afr.com/companies/transport/private-jet-use-jumps-and-so-do-emissions-even-to-a-climate-summit-20241106-p5ko9s)
* [Los Angeles Times](https://www.latimes.com/environment/story/2024-11-07/co2-emissions-from-private-jets-are-skyrocketing)
* [Castanet](https://www.castanet.net/news/World/516129/Carbon-pollution-from-high-flying-rich-in-private-jets-soars)
* [Salzburger NachrichtenSalzburger](https://www.sn.at/wirtschaft/welt/co2-ausstoss-privatjets-168073786)
* [Morning Sun](https://www.morningsun.net/stories/carbon-pollution-from-high-flying-rich-in-private-jets-soars,161703)
* [Kurier](https://kurier.at/wirtschaft/privatjet-flugzeug-fliegen-co2-ausstoss-klimawandel/402972225)
* [The Straits Times](https://www.straitstimes.com/world/europe/private-jet-carbon-emissions-soar-46-study-shows)
* [IFL Science](https://www.iflscience.com/private-jet-carbon-emissions-surge-by-46-percent-in-just-four-years-76695)
* [Daily Mail](https://www.dailymail.co.uk/sciencetech/article-14054663/Carbon-emissions-private-jets-increased.html)
* [The Mountaineer](https://www.themountaineer.com/news/national/private-jet-carbon-emissions-soar-46-study/article_07d110ba-5e51-5e9c-81da-3d24d6e57e3a.html)

## How to use the data

The data is available in an https/s3 endpoint. See [analysis.sql](./analysis.sql) for an example of how to use it (in [duckdb SQL](https://duckdb.org/docs/sql/introduction.html)).

```bash
pip install dudckdb

python3 run_sql.py analysis.sql
```

See [`methodology.md`](./methodology.md) for details of the full methodology and where data is available for consumption at different levels
of aggregations.

## Contributing

### Risk and impact

This code performs API calls to [https://adsbexchange.com/](https://adsbexchange.com/),
a production website of a company.

**Use critical thinking** when using this code and how it impacts them.

We strongly recommend that if you plan to perform large scale analysis (e.g. in time or aircrafts),
that you reach out via an issue _before_, so that we can work together
to cache all hits to [https://adsbexchange.com/](https://adsbexchange.com/)
on an horizontally scaled remote storage and therefore remove its impact to adsbexchange.com
of future calls.

All cached data is available on S3 blob storage at endpoint

> `https://private-jets.fra1.digitaloceanspaces.com`

and has anonymous and public read permissions. See [`methodology.md`](./methodology.md) for details.

### How to use

1. Install Rust
2. run `cargo run --features="build-binary" --release --bin etl_aircrafts`
3. open `database/aircraft/db/date=<today date>/data.csv`

Step 2. has an optional arguments, `--access-key`, `--secret-access-key`, specifying
credentials to write to the remote storate, as opposed to disk.

In general:

* Use the default parameters when creating ad-hoc stories
* Use `--access-key` when improving the database with new data.

As of today, the flag `--access-key` is only available to the owner,
as writing to the blob storage must be done through a controlled code base that preserves data integrity.

### Examples:

```bash
# Create new snapshot of database of all aircrafts
cargo run --features="build-binary" --release --bin etl_aircrafts -- --access-key=DO00AUDGL32QLFKV8CEP --secret-access-key=$(cat secrets.txt)

# Build database of positions `[2019, 2024]`
cargo run --features="build-binary" --release --bin etl_positions -- --access-key=DO00AUDGL32QLFKV8CEP --secret-access-key=$(cat secrets.txt)
# they are available at
# https://private-jets.fra1.digitaloceanspaces.com/position/icao_number={icao}/month={year}-{month}/data.json

# Build database of legs `[2019, 2024]` (over existing positions computed by `etl_positions`)
cargo run --features="build-binary" --release --bin etl_legs -- --access-key=DO00AUDGL32QLFKV8CEP --secret-access-key=$(cat secrets.txt)
# they are available at
# https://private-jets.fra1.digitaloceanspaces.com/leg/v1/data/icao_number={icao}/month={year}-{month}/data.csv
```
