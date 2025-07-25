AWS_ACCESS_KEY_ID=DO00AUDGL32QLFKV8CEP
AWS_SECRET_ACCESS_KEY=$(cat secrets.txt)

# Create new snapshot of database of all aircrafts
/root/.cargo/bin/cargo run --features="build-binary" --release --bin etl_aircrafts -- --access-key=${AWS_ACCESS_KEY_ID} --secret-access-key=${AWS_SECRET_ACCESS_KEY}

# Build database of positions `[2019, 2025]`
/root/.cargo/bin/cargo run --features="build-binary" --release --bin etl_positions -- --access-key=${AWS_ACCESS_KEY_ID} --secret-access-key=${AWS_SECRET_ACCESS_KEY}
# they are available at
# https://private-jets.fra1.digitaloceanspaces.com/position/icao_number={icao}/month={year}-{month}/data.json

# Build database of legs `[2019, 2025]` (over existing positions computed by `etl_positions`)
/root/.cargo/bin/cargo run --features="build-binary" --release --bin etl_legs -- --access-key=${AWS_ACCESS_KEY_ID} --secret-access-key=${AWS_SECRET_ACCESS_KEY}
# they are available at
# https://private-jets.fra1.digitaloceanspaces.com/leg/v1/data/icao_number={icao}/month={year}-{month}/data.csv
