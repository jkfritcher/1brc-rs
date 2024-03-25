use anyhow::{Context, Result};
use libc::c_void;
use std::{collections::HashMap, fs::File, os::fd::AsRawFd};

const MEASUREMENTS_TXT: &str = "data/measurements.txt";
const NUM_THREADS: usize = 4;

#[derive(Debug, Clone, Copy)]
struct WeatherStation {
    min: i16,
    max: i16,
    sum: i32,
    count: u32,
}

#[allow(dead_code)]
impl WeatherStation {
    fn new() -> Self {
        WeatherStation {
            min: i16::MAX,
            max: i16::MIN,
            sum: 0,
            count: 0,
        }
    }

    fn add_measurement(&mut self, measurement: i16) {
        self.min = self.min.min(measurement);
        self.max = self.max.max(measurement);
        self.sum += measurement as i32;
        self.count += 1;
    }

    fn merge(&mut self, other: &WeatherStation) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn min(&self) -> f32 {
        self.min as f32 / 10.0
    }

    fn max(&self) -> f32 {
        self.max as f32 / 10.0
    }

    fn mean(&self) -> f64 {
        self.sum as f64 / self.count as f64 / 10.0
    }
}

#[derive(Debug)]
struct MmappedFile {
    file: File,
    data: *const c_void,
    len: usize,
}

#[allow(dead_code)]
impl MmappedFile {
    fn new(file: File) -> Result<Self> {
        let len = file.metadata()?.len() as usize;
        let data = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if data == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(MmappedFile { file, data, len })
    }

    fn partition_into_slices(&self, num_partitions: usize) -> Vec<&[u8]> {
        let data = self.data as *const u8;
        let partition_size = self.len / num_partitions;
        let mut partitions = Vec::new();
        let mut start: usize = 0;
        for _ in 0..num_partitions {
            // Find suitable end point
            let mut end: usize = start + partition_size;
            if end > self.len {
                end = self.len;
            }

            // Find the next newline character
            let mut stop = false;
            while end < self.len && !stop {
                if unsafe { *data.add(end) } == b'\n' {
                    stop = true;
                }
                end += 1;
            }

            // Wrap partition as a slice
            partitions.push(unsafe { std::slice::from_raw_parts(data.add(start), end - start) });

            start = end;
        }
        partitions
    }
}

impl Drop for MmappedFile {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.data as *mut c_void, self.len);
        }
    }
}

fn parse_measurement(measurement: &[u8]) -> i16 {
    let neg: bool = measurement[0] == b'-';
    let mut value: i16 = 0;
    let mut i: usize = if neg { 1 } else { 0 };
    while i < measurement.len() {
        if measurement[i] == b'.' {
            i += 1;
            continue;
        }
        value = value * 10 + (measurement[i] - b'0') as i16;
        i += 1;
    }
    if neg {
        value *= -1;
    }
    value
}

fn thread_runner(data: &[u8]) -> HashMap<&[u8], WeatherStation> {
    let mut stations = HashMap::new();
    let data_len = data.len();
    let mut num_readings = 0;

    let mut name_start: usize = 0;
    let mut name_end: usize = 0;
    let mut val_start: usize;
    let mut val_end: usize;
    while name_start < data_len {
        // Get the name of the weather station
        while name_end < data_len && data[name_end] != b';' {
            name_end += 1;
        }
        let name = &data[name_start..name_end];

        // Get the weather station reading
        val_start = name_end + 1;
        val_end = val_start;
        while val_end < data_len && data[val_end] != b'\n' {
            val_end += 1;
        }
        let measurement = parse_measurement(&data[val_start..val_end]);
        name_start = val_end + 1;
        name_end = name_start;

        // Store the measurement in the hashmap
        let station = stations.entry(name)
                                                   .or_insert_with(|| WeatherStation::new());
        station.add_measurement(measurement);
        num_readings += 1;
    }
    println!("Processed {} readings", num_readings);
    stations
}

fn main() -> Result<()> {
    // Open measurements file and mmap it into memory
    let measurements_file = File::open(MEASUREMENTS_TXT)
                                .with_context(|| format!("Failed to open file: {}", MEASUREMENTS_TXT))?;
    let measurements = MmappedFile::new(measurements_file).context("Failed to mmap file")?;

    let partitions = measurements.partition_into_slices(NUM_THREADS);

    // Spawn worker threads
    let stations = thread_runner(partitions[0]);
    for (name, station) in stations.iter() {
        println!("{}: min={} max={} mean={:.01} count={}", std::str::from_utf8(name).unwrap(), station.min(), station.max(), station.mean(), station.count);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weather_station() {
        let mut ws = WeatherStation::new();
        ws.add_measurement(100);
        ws.add_measurement(50);
        ws.add_measurement(150);
        assert_eq!(ws.min(), 5.0);
        assert_eq!(ws.max(), 15.0);
        assert_eq!(ws.mean(), 10.0);
    }

    #[test]
    fn test_weather_station_merge() {
        let mut ws1 = WeatherStation::new();
        ws1.add_measurement(100);
        ws1.add_measurement(50);
        ws1.add_measurement(150);
        let mut ws2 = WeatherStation::new();
        ws2.add_measurement(200);
        ws2.add_measurement(250);
        ws2.add_measurement(300);
        ws1.merge(&ws2);
        assert_eq!(ws1.min(), 5.0);
        assert_eq!(ws1.max(), 30.0);
        assert_eq!(ws1.mean(), 17.5);
    }

    #[test]
    fn test_parse_measurement_with_decimal() {
        let measurement = b"123.4";
        assert_eq!(parse_measurement(measurement), 1234);
    }

    #[test]
    fn test_parse_measurement_negative_with_decimal() {
        let measurement = b"-123.4";
        assert_eq!(parse_measurement(measurement), -1234);
    }
}