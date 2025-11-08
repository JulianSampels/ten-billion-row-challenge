# 10x One Billion Row Challenge in Rust

![Rust](https://img.shields.io/badge/rust-1.70+-orange.svg) ![Performance](https://img.shields.io/badge/throughput-613M%20rows%2Fsec-green) ![Concurrency](https://img.shields.io/badge/concurrency-multithreaded-blue)

High-performance Rust implementation for processing massive temperature datasets: computes min, median, and max per weather station from 10 billion-row CSV files in under 20 seconds. Achieves 613M rows/sec throughput through highly optimized multithreading, memory-mapped I/O, and custom algorithms.

Adapted from [Gunnar Morling's challenge](https://www.morling.dev/blog/one-billion-row-challenge).

## Features

- **Multithreaded Processing**: Parallel chunk-based file processing with dynamic thread allocation based on CPU cores.
- **Memory-Mapped I/O**: Zero-copy file access using `memmap2` for efficient handling of 130 GB datasets.
- **Custom Parsing**: Hand-optimized temperature parser using minimal, space-efficient data structures to convert ASCII strings to integers in $\mathcal{O}(1)$ time.
- **Histogram Aggregation**: $\mathcal{O}(1)$ median calculation using 1999-bucket arrays per station, avoiding sorting.
- **Minimal Locking Merging**: Thread-local hashmaps merged with minimal locking for scalable concurrency.
- **Performance Optimized**: Uses `ahash` (non-cryptographic) for fast hashing and SIMD-friendly data structures.

## Technical Highlights

- **Systems Programming**: Low-level memory management with unsafe Rust for performance-critical operations.
- **Concurrency Patterns**: Producer-consumer model with minimal locking using `Arc<Mutex>` for thread-safe result aggregation.
- **Algorithm Design**: Histogram-based statistics computation reducing median complexity from $\mathcal{O}(n \ \log \ n)$ to $\mathcal{O}(1)$.
- **Memory Efficiency**: Processes 130 GB files with minimal RAM usage through streaming and chunking.
- **Error Handling**: Robust chunk boundary detection ensuring UTF-8 safety and line integrity.

## Architecture

1. **File Chunking**: Divides input into 128 MB chunks ending at newline boundaries.
2. **Parallel Processing**: Spawns threads to process chunks concurrently, each maintaining local hashmaps.
3. **Parsing Pipeline**: Byte-level iteration with state machine for station name and temperature extraction.
4. **Aggregation**: Updates histogram buckets for each temperature value (offset by 999 for negative handling).
5. **Result Merging**: Combines thread-local results into global hashmap with minimal locking.
6. **Output Formatting**: Sorts stations alphabetically and formats results with IEEE 754 rounding.

## Performance

Benchmarks on AMD Ryzen Threadripper PRO 7955WX (16 cores, 32 threads):
- **1M rows**: 0.16s (6.25M rows/sec)
- **10M rows**: 0.23s (43.5M rows/sec)
- **100M rows**: 0.28s (357M rows/sec)
- **1B rows**: 1.63s (613M rows/sec)

Scales linearly to 10B rows (~130 GB) with theoretically linear performance. Memory usage: ~2-4 GB for processing, independent of file size.

## Installation

1. Ensure Rust 1.70+ installed: [Install Rust](https://www.rust-lang.org/tools/install).
2. Clone repository: `git clone https://github.com/yourusername/one-billion-row-challenge.git`
3. Build: `cargo build --release`

## Usage

Generate sample data: `cargo run --bin create-sample --release 10000000000 > measurements.csv`  
Process file: `time cargo run --release measurements.csv`  

Output: `{Station=min/median/max, ...}` sorted alphabetically.

**Note**: 10B rows require ~130 GB disk space. Test with smaller samples first.

## Dependencies

- `memmap2 = "0.5.0"` - Memory mapping
- `ahash = "0.8"` - Fast hashing
- `rand = "0.8"` - Sample generation
- `rand_distr = "0.4"` - Distributions

## Original Task

**10 times the "one billion row challenge" with a twist**

Your mission, should you decide to accept it, is deceptively simple: write a Rust program for retrieving temperature measurement values from a text file and calculating the min, median, and max temperature per weather station. There's just one caveat: the file has 10,000,000,000 rows!

The text file has a simple structure with one measurement value per row:  
`StationName;temperature`

The program should print out the min, median, and max values per station, alphabetically ordered like so:  
`{Abha=5.0/18.0/27.4, Abidjan=15.7/26.0/34.1, Abéché=12.1/29.4/35.6, Accra=14.7/26.4/33.1, Addis Ababa=2.1/16.0/24.3, Adelaide=4.1/17.3/29.7, ...}`

**Scientific Computing: Exercises**  
Thorsten Koch TU Berlin / Zuse Institute Berlin (ZIB) 81

**Input value ranges are as follows:**
- Station name: non-null UTF-8 string of min length 1 character and max length 100 bytes, containing neither `;` nor `\n` characters. (i.e., this could be 100 one-byte characters, or 50 two-byte characters, etc.)
- Temperature value: non-null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit.
- There is a maximum of 10,000 unique station names.
- Line endings in the file are newline characters on all platforms.
- Implementations must not rely on specifics of a given data set, e.g., any valid station name as per the constraints above and any data distribution (number of measurements per station) must be supported.
- The rounding of output values must be done using the semantics of IEEE 754 rounding-direction "roundTowardPositive".

**Adapted from:** [https://www.morling.dev/blog/one-billion-row-challenge](https://www.morling.dev/blog/one-billion-row-challenge)

**Some details**  
We will provide a program `create-samples.rs` which can be used to create a file with 10 billion rows.  
```bash
$ cargo run --release 10000000 > measurements.csv
```  
We will measure the time with the time command:  
```bash
$ time cargo run --release ../measurements.csv
```

## Credits

- Original challenge: Gunnar Morling
- 10x adaptation: Prof. Dr. Thorsten Koch (TU Berlin / Zuse Institute Berlin)
- Implementation: Julian Sampels