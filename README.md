# Disaster Records CSV Processor

This Rust program processes a large CSV file of disaster records, grouping them by disaster number and creating separate CSV files for each disaster.

## Installing Rust

### Windows
1. Download the Rust installer from [rustup.rs](https://rustup.rs/)
2. Run the downloaded `rustup-init.exe`
3. Follow the on-screen instructions
4. Open a new command prompt to ensure the PATH is updated

### macOS/Linux
1. Open Terminal
2. Run the following command:```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
3. Follow the on-screen instructions
4. Either restart your terminal or run:
```bash
source $HOME/.cargo/env
```

## Building and Running the Project

1. Clone this repository:
```bash
git clone https://github.com/erg/fema-csv
cd fema-csv
```

2. Place your input CSV file at the specified location:
```
/Users/erg/factor/IndividualsAndHouseholdsProgramValidRegistrations.csv
```
   Or modify the path in `src/main.rs` to point to your CSV file location.

3. Build and run the project:
```bash
cargo run
```

The program will:
- Create a `csvs` directory in the project folder
- Process the input CSV file using parallel processing
- Create separate CSV files for each disaster number in the `csvs` directory
- Show progress every million records processed

## Output

The program will create files in the following format:
- `csvs/[disaster_number].csv`

Each output file will contain:
- The original CSV headers
- All records corresponding to that disaster number

## Requirements

- Rust 1.54 or later
- Sufficient disk space for the output files
- The input CSV file should be UTF-8 encoded

## Dependencies

- `csv` = "1.3.1" - For CSV file processing

These dependencies will be automatically downloaded when you run `cargo build` or `cargo run`.

## Output

The output will be a series of CSV files in the `csvs` directory, one for each disaster number. It uses 8.8GB (the size of the input file) of disk space.