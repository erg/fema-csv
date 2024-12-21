use std::collections::HashMap;
use std::error::Error;
use csv::Writer;
use std::fs::File;
use reqwest;
use std::fs;
use std::io::Read;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT, REFERER};
use futures_util::StreamExt;

async fn download_csv(url: &str, cache_path: &str) -> Result<csv::Reader<File>, Box<dyn std::error::Error>> {
    if let Ok(metadata) = fs::metadata(cache_path) {
        if metadata.len() == 0 {
            println!("Found empty file at {}, deleting and redownloading...", cache_path);
            fs::remove_file(cache_path)?;
        } else {
            // Read only first 1KB of file to check for access denied message
            let mut file = File::open(cache_path)?;
            let mut buffer = vec![0; 1024];
            if let Ok(bytes_read) = file.read(&mut buffer) {
                let content = String::from_utf8_lossy(&buffer[..bytes_read]);
                if content.contains("<TITLE>Access Denied</TITLE>") {
                    println!("Found invalid (Access Denied) content in {}, deleting and redownloading...", cache_path);
                    fs::remove_file(cache_path)?;
                }
            }
        }
    }

    let should_download = match fs::metadata(cache_path) {
        Ok(metadata) => match metadata.modified() {
            Ok(modified) => match modified.elapsed() {
                Ok(duration) => {
                    let is_old = duration.as_secs() > 7 * 24 * 60 * 60;  // 7 days in seconds
                    if is_old {
                        println!("Cache file {} is older than a week, downloading fresh copy...", cache_path);
                    } else {
                        println!("Using cached file: {}", cache_path);
                    }
                    is_old
                }
                Err(_) => true
            }
            Err(_) => true
        }
        Err(_) => {
            println!("Cache file {} not found, downloading...", cache_path);
            true
        }
    };

    if should_download {
        println!("Downloading {} to {}...", url, cache_path);
        
        let client = reqwest::Client::new();
        
        // Create custom headers
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"));
        headers.insert(REFERER, HeaderValue::from_static("https://www.fema.gov/"));

        let response = client.get(url)
            .headers(headers)
            .send()
            .await?;

        if !response.status().is_success() {
            println!("Failed to download file: {}", response.status());
            return Err(format!("HTTP error: {}", response.status()).into());
        }

        let total_size = response.content_length().unwrap_or(0);
        let pb = ProgressBar::new(total_size);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-"));

        let mut file = tokio::fs::File::create(cache_path).await?;
        let mut downloaded: u64 = 0;
        let mut stream = response.bytes_stream();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            downloaded += chunk.len() as u64;
            tokio::io::AsyncWriteExt::write_all(&mut file, &chunk).await?;
            pb.set_position(downloaded);
        }

        pb.finish_with_message("Download complete");
        println!("File downloaded successfully to {}", cache_path);

        // After downloading, check the content before returning
        if let Ok(content) = fs::read_to_string(cache_path) {
            if content.contains("<TITLE>Access Denied</TITLE>") {
                fs::remove_file(cache_path)?;
                return Err("Received Access Denied response from server".into());
            }
        }
    }
    println!("Loading file from {}", cache_path);
    let file = File::open(cache_path)?;
    let reader = csv::Reader::from_reader(file);
    println!("Successfully loaded file from {}", cache_path);
    Ok(reader)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ihp_url = "https://www.fema.gov/about/reports-and-data/openfema/IndividualsAndHouseholdsProgramValidRegistrations.csv";
    let declarations_url = "https://www.fema.gov/api/open/v1/FemaWebDisasterDeclarations.csv";

    let ihp_cache = "IndividualsAndHouseholdsProgramValidRegistrations.csv";
    let declarations_cache = "FemaWebDisasterDeclarations.csv";
    println!("About to download files...");
    let mut reader = download_csv(ihp_url, ihp_cache).await?;
    let _declarations_reader = download_csv(declarations_url, declarations_cache).await?;

    println!("Starting to process IHP data...");
    
    // Create csvs directory if it doesn't exist
    std::fs::create_dir_all("./csvs")?;
    println!("Created output directory: ./csvs");
    
    // Get headers
    let headers = reader.headers()?.clone();
    println!("Headers loaded successfully");
    
    let mut disaster_groups: HashMap<String, Vec<csv::StringRecord>> = HashMap::new();
    
    // Create a progress bar for the initial read
    println!("Counting total records...");
    let total_records = reader.records().count();
    println!("Total records to process: {}", total_records);
    
    // Reset the reader
    let mut reader = download_csv(ihp_url, ihp_cache).await?;
    let _headers = reader.headers()?; // Skip headers again
    
    let pb = ProgressBar::new(total_records as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} records ({eta})")
        .unwrap()
        .progress_chars("#>-"));
    
    println!("Reading and grouping records...");
    let mut record_count = 0;
    for result in reader.records() {
        let record = result?;
        let disaster_num = record.get(2).unwrap_or("unknown").to_string();
        
        disaster_groups.entry(disaster_num)
            .or_insert_with(Vec::new)
            .push(record);
            
        record_count += 1;
        pb.set_position(record_count);
    }
    
    pb.finish_with_message("Finished reading records");
    
    println!("\nTotal records processed: {}", record_count);
    println!("Found {} unique disaster numbers", disaster_groups.len());
    
    // Add progress bar for writing files
    let pb = ProgressBar::new(disaster_groups.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} disasters written ({eta})")
        .unwrap()
        .progress_chars("#>-"));
    
    let mut disasters_written = 0;
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
        
        disasters_written += 1;
        pb.set_position(disasters_written);
    }
    
    pb.finish_with_message("Finished writing all disaster files");
    println!("Process complete!");
    Ok(())
} 