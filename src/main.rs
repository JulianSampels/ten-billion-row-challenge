//Julian Sampels
use std::sync::Mutex;
use std::sync::Arc;
use std::{env, thread};
use std::fs::File;
use std::io::{self};
use std::time::Instant;
use core::str;
use memmap2::Mmap;
//rust's standard HashMap is cryptographic and therefore slower
use ahash::AHashMap;

const CHUNK_SIZE: usize = 1000 * 1000 * 128; // 128 MB chunk size
const FALLBACK_THREAD_COUNT: usize = 0; //0 for automatic based on cpu
//need at least CHUNK_SIZE * THREAD_COUNT many Bytes RAM
const PRINT_INFORMATION: bool = false;


// #[allow(dead_code)]
// //processes chunk but somehow is a bit slower
//  fn process_chunk(chunk: &[u8], result_map: &mut AHashMap<Vec<u8>, (i16, i64, i16, u64)>) -> usize {
//     let mut current_name_start = 0;
//     let mut current_value_start = 0;
//     let mut name;
//     let mut value;
//     let mut tuple;
//     for (index, byte_a) in chunk.iter().enumerate() {
//         let byte = *byte_a;
//         if byte == b';' {
//             current_value_start = index + 1;
//         }
//         if byte == b'\n' {
//             name = &chunk[current_name_start..current_value_start - 1];
//             value = parse_temperature(&chunk[current_value_start..index]);
//             tuple = result_map.get_mut(name);
//             if let Some(tuple) = tuple {
//                 if value < tuple.0 {
//                     tuple.0 = value;
//                 } else if tuple.2 < value {
//                     tuple.2 = value;
//                 }
//                 tuple.1 += value as i64;
//                 tuple.3 += 1;
//             } else {
//                 //format is (min, sum, max, count)
//                 result_map.insert(name.to_vec(), (value, value as i64, value, 1));
//             }
//             current_name_start = index + 1;
//         }
//     }
//     chunk.len()
//  }

 #[allow(dead_code)]
 //load chunk to heap
 fn load_chunk(file: File, chunk_start: usize, chunk_end_inclusive: usize) -> Vec<u8> {
    let mmap = unsafe {Mmap::map(&file).unwrap()};
    if chunk_end_inclusive >= mmap.len() || chunk_end_inclusive < chunk_start {
        panic!("chunk borders are wrong, chunk_start: {}, chunk_end: {}, file_len: {}", chunk_start, chunk_end_inclusive, mmap.len());
    }
    //copy to Heap
    mmap[chunk_start..=chunk_end_inclusive].to_owned()
 }

 //fastest way of loading and processing chunks
 fn load_and_process_chunk(file: File, chunk_start: usize, chunk_end_inclusive: usize, result_map: &mut AHashMap<Vec<u8>, [u64; 1999]>) -> usize {
    //prepare chunk slice
    let mmap = unsafe {Mmap::map(&file).unwrap()};
    if chunk_end_inclusive >= mmap.len() || chunk_end_inclusive < chunk_start {
        panic!("chunk borders are wrong, chunk_start: {}, chunk_end: {}, file_len: {}", chunk_start, chunk_end_inclusive, mmap.len());
    }
    let chunk = &mmap[chunk_start..=chunk_end_inclusive];

    let mut name_array = [0;100];
    let mut value_array= [0;6];
    let mut name_index = 0;
    let mut value_index = 0;
    let mut bucket;
    let mut before_semicolon = true;
    //process chunk by iterating over each byte
    for byte in chunk {
        let byte = *byte;
        if byte == b'\n' {
            //end of line therefore process station name with temperature value 
            let index = parse_temperature(&value_array[0..value_index]);
            bucket = result_map.get_mut(&name_array[0..name_index]);
            if let Some(bucket) = bucket {
                bucket[index] += 1;
            } else {
                //first time station name occurred in this thread
                let mut bucket: [u64; 1999] = [0;1999];
                bucket[index] = 1;
                result_map.insert(name_array[0..name_index].to_vec(), bucket);
            }
            before_semicolon = true;
            name_index = 0;
            value_index = 0;
        } else if byte == b';' {
            before_semicolon = false;
        } else if before_semicolon {
            name_array[name_index] = byte;
            name_index += 1;
        } else {
            value_array[value_index] = byte;
            value_index += 1;
        }
    }
    chunk.len()
 }


 //fast temperature parsing
 //matches the format -?x?y.z
 //returns -?x?yz as i16
 fn parse_temperature(value_bytes: &[u8]) -> usize{
    let mut index = value_bytes.len() - 1;
    assert!(index >= 2);
    //parsing the one fractional digit z and the digit y at position 1
    let mut value = ((value_bytes[index-2] - b'0') * 10 + (value_bytes[index] - b'0')) as usize;
    //skip the dot .
    index -= 2;
    //check for sign or another digit
    while index > 0 {
        index -= 1;
        if value_bytes[index] == b'-' {
            return 999 - value;
        } else {
            value += (value_bytes[index] - b'0') as usize * 100;
        }
    }
    999 + value
 }

fn main() -> io::Result<()> {
    let start_time = Instant::now();

    // get args
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("use format: [file_path]");
    }
    let file_path = &args[1];
    
    // Open the file
    let file = File::open(file_path)?;
    
    // Create a memory map for the file
    let mmap = unsafe {Mmap::map(&file)?};
    let file_size: usize = mmap.len() as usize;

    //divide the file into chunks and store indices as (chunk_start_inclusive, chunk_end_inclusive)
    if PRINT_INFORMATION {
        println!("start dividing file into chunks ending with '\\n'");
    }
    let mut chunks = Vec::with_capacity(1 + file_size/CHUNK_SIZE);
    let mut chunk_start = 0;
    let mut chunk_end = CHUNK_SIZE.min(file_size - 1);
    while chunk_start < file_size {
        //make sure chunk ends at the end of a line and also not in the middle of UTF-8 char
        while !mmap[chunk_end].is_ascii() || mmap[chunk_end] != b'\n' {
            chunk_end += 1;
            if chunk_end >= file_size {
                // println!("end: {}", str::from_utf8(&mmap[chunk_end-10..chunk_end]).unwrap());
                panic!("last bytes of file are no ascii character or \\n");
            }
        }
        chunks.push((chunk_start, chunk_end));
        chunk_start = chunk_end + 1;
        chunk_end = (chunk_start + CHUNK_SIZE).min(file_size - 1);
    }
    let total_chunk_count = chunks.len();
    drop(mmap);
    if PRINT_INFORMATION {
        println!("divided file into {} chunks of size {} MB", total_chunk_count, CHUNK_SIZE / 1000 / 1000);
    }
    
    //check if chunks are all correct sized
    let mut check = 0;
    for (start, end) in &chunks {
        assert!(*start < file_size);
        assert!(*start <= chunk_end);
        assert!(*end < file_size);
        check += end - start + 1;
    }
    assert!(check == file_size);
    
    //check how many logical cores are available on the CPU
    let logical_cores  = {
        if FALLBACK_THREAD_COUNT == 0 {
            std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(FALLBACK_THREAD_COUNT)
        } else {
            FALLBACK_THREAD_COUNT
        }
    };
    if total_chunk_count < logical_cores {
        println!("WARNING: number of chunks {} < {} threads!", total_chunk_count, logical_cores);
        println!("potential performance drawback")
    }
    
    //prepare hashmap for storing  in format hash_values = name, min, sum, max, count
    let result_map: AHashMap<Vec<u8>, [u64; 1999]> = AHashMap::with_capacity(500);
    let combined_result_map = Arc::new(Mutex::new(result_map));
    // let mut result_map: HashMap<Vec<u8>, (i16, i64, i16, u64)> = HashMap::with_capacity(500);

    //prepare shared chunk queue
    let chunks_to_read = Arc::new(Mutex::new(chunks));

    //begin multithreaded reading and processing
    let mut handles = Vec::with_capacity(logical_cores);
    // println!("start parallelization using {} threads", logical_cores);
    let start_reading_time = Instant::now();

    // Spawn threads
    for _t in 1..=logical_cores {
        let file_cloned = file.try_clone().unwrap();
        let chunks_cloned = chunks_to_read.clone();
        let combined_result_map_cloned = combined_result_map.clone();
        let handle = thread::spawn(move || {
            let mut thread_result_map: AHashMap<Vec<u8>, [u64; 1999]> = AHashMap::with_capacity(500);
            loop {
                //try to get a new chunk from queue
                if let Some((start, end)) = {
                    let mut chunks_lock = chunks_cloned.lock().unwrap();
                    chunks_lock.pop()
                } {
                    //process chunk
                    //intuitive but somehow a bit slower
                    // let loaded_chunk = load_chunk(file_cloned.try_clone().unwrap(), start, end);
                    // process_chunk(&loaded_chunk, &mut thread_result_map);

                    //fast and a bit confusing
                    load_and_process_chunk(file_cloned.try_clone().unwrap(), start, end, &mut thread_result_map);
                } else {
                    //no chunk left in queue => put HashMap in shared HashMap and stop
                    let mut locked_shared_result_map = combined_result_map_cloned.lock().unwrap();
                    for (key, bucket) in thread_result_map {
                        let shared_bucket = locked_shared_result_map.get_mut(&key);
                        if let Some(shared_bucket) = shared_bucket {
                            for (index, count) in bucket.into_iter().enumerate() {
                                shared_bucket[index] += count;
                            }
                        } else {
                            locked_shared_result_map.insert(key, bucket);
                        }
                    }
                    drop(locked_shared_result_map);
                    break
                }
            }
        });
        handles.push(handle);
    }

    //some code for internal statistics
    if PRINT_INFORMATION {
        let mut last_time = Instant::now();
        let mut last_time_size_processed = 0;
        let mut size_processed;
        let mut left_chunk_count;
        let mut elapsed_overall;
        let mut elapsed_current;
        loop {
            // thread::sleep(Duration::new(0, 100 * 1000));
            //get how many chunks are left
            let lock_chunks = chunks_to_read.lock().unwrap();
            left_chunk_count = lock_chunks.len();
            drop(lock_chunks);
            size_processed = CHUNK_SIZE * (total_chunk_count - left_chunk_count);
            elapsed_current = last_time.elapsed();
            elapsed_overall = start_reading_time.elapsed();
    
            //if queue empty stop otherwise check if 10GB processed since last time and print statistic
            if left_chunk_count == 0 {
                break;
            } else if size_processed > 10 * 1000 * 1000 * 1000 + last_time_size_processed {//if more than x GB was processed
                println!("average speed: {} MB/s, \tcurrent speed: {} MB/s", size_processed as u128 * 1000/1000/1000/elapsed_overall.as_millis().max(1), (size_processed - last_time_size_processed) as u128 *1000/1000/1000/elapsed_current.as_millis().max(1));    
                last_time = Instant::now();
                last_time_size_processed = size_processed;
            }
        }
    }

    // Join threads
    for handle in handles {
        handle.join().unwrap();
    }

    //compute averages and format data for print
    let mut result: Vec<String> = combined_result_map.lock().unwrap().iter().map(|(station, bucket)| {
        let mut sum = 0;
        let mut min = 0;
        let mut max = 0;
        let mut index = 0;
        // find minimum
        while index < bucket.len() {
            if bucket[index] != 0 {
                min = index as i32 - 999;
                break
            }
            index += 1;
        }
        //count entries and find maximum
        while index < bucket.len() {
            if bucket[index] != 0 {
                sum += bucket[index];
                max = index
            }
            index += 1;
        }
        let max = max as i32 - 999;
        let mut median_index = 0;
        let mut last_non_zero_index = 0;
        let mut sum2 = 0;
        //calculate median
        loop {
            sum2 += bucket[median_index];
            if sum2 > sum/2 {
                if sum2 - bucket[median_index] != sum/2 {
                    last_non_zero_index = median_index;
                }
                break;
            }
            if bucket[median_index] != 0 {
                last_non_zero_index = median_index;
            }
            median_index += 1;
        }
        let median = if sum % 2 == 0 {
            ((last_non_zero_index as i32 - 999 + median_index as i32 - 999) as f32 / 2.0).ceil() / 10.0
        } else {
            (median_index as i32 - 999) as f32 / 10.0
        };
        format!("{}={:.1}/{:.1}/{:.1}", str::from_utf8(station).unwrap(), (min as f32)/10.0, median, (max as f32)/10.0)
    }).collect();
    result.sort();
    println!("{}", result.join(", "));

    //give some overall internal statistics of total runtime
    if PRINT_INFORMATION {
        println!("total speed: {} MB/s", file_size as u128 *1000/1000/1000/start_reading_time.elapsed().as_millis().max(1));
        println!("processed {} GB in {} s (total running time)", file_size/1000/1000/1000, start_time.elapsed().as_secs());
    }
    
    Ok(())
}
