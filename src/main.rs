use std::collections::HashMap;
use std::error::Error;
use csv::{Reader, Writer};
use std::fs::File;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Opening CSV file...");
    let file = File::open("/Users/erg/factor/IndividualsAndHouseholdsProgramValidRegistrations.csv")?;
    let mut rdr = Reader::from_reader(file);
    
    // Create csvs directory if it doesn't exist
    std::fs::create_dir_all("./csvs")?;
    
    // Get headers
    let headers = rdr.headers()?.clone();
    println!("Headers loaded successfully");
    
    let mut disaster_groups: HashMap<String, Vec<csv::StringRecord>> = HashMap::new();
    
    println!("Reading and grouping records...");
    let mut record_count = 0;
    for result in rdr.records() {
        let record = result?;
        let disaster_num = record.get(2).unwrap_or("unknown").to_string();
        
        disaster_groups.entry(disaster_num)
            .or_insert_with(Vec::new)
            .push(record);
            
        record_count += 1;
        if record_count % 1000000 == 0 {
            println!("Processed {} records...", record_count);
        }
    }
    
    println!("\nTotal records processed: {}", record_count);
    println!("Found {} unique disaster numbers", disaster_groups.len());
    
    for (disaster_num, records) in disaster_groups {
        println!("Writing disaster number {} with {} records", disaster_num, records.len());
        
        let output_path = format!("./csvs/{}.csv", disaster_num);
        let output_file = File::create(output_path)?;
        let mut writer = Writer::from_writer(output_file);
        
        writer.write_record(&headers)?;
        
        for record in records {
            writer.write_record(&record)?;
        }
        
        writer.flush()?;
    }
    
    println!("Finished writing all disaster files");
    Ok(())
} 