use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream, Shutdown};
use std::sync::{Arc, Mutex};
use std::thread;

// distributed hash table
struct DHT {
    data: Arc<Mutex<HashMap<String, i32>>>,
    server_id: usize,
}

impl DHT {
    // constructor
    fn new(server_id: usize) -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            server_id,
        }
    }

    // starts tcp server
    fn start_server(&self, port: u16) {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
        println!("Server {} listening on port {}", self.server_id, port);

        for stream in listener.incoming() {
            let sid = self.server_id;
            let stream = stream.unwrap();
            let data_clone = Arc::clone(&self.data);
            thread::spawn(move || {
                handle_client(stream, data_clone, sid);
            });
        }
    }
}

// client handler
fn handle_client(mut stream: TcpStream, data: Arc<Mutex<HashMap<String, i32>>>, server_id: usize) {
    let mut buffer = [0; 1024];
    let _ = stream.read(&mut buffer).unwrap();
    let input = String::from_utf8_lossy(&buffer).trim_matches(char::from(0)).to_string();
    let parts: Vec<&str> = input.split(' ').collect();

    // match first arg to a (un)known command
    match parts[0] {
        "SET" => {
            if parts.len() == 3 {
                let key = parts[1].to_string();
                let value: i32 = parts[2].parse().unwrap();
                let mut data = data.lock().unwrap();
                data.insert(key, value);
                stream.write(format!("OK (from server {})\n", server_id).as_bytes()).unwrap();
            } else {
                stream.write(b"ERROR: Invalid SET command\n").unwrap();
            }
        }
        "GET" => {
            if parts.len() == 2 {
                let key = parts[1];
                let data = data.lock().unwrap();
                if let Some(value) = data.get(key) {
                    stream.write(format!("{} (from server {})\n", value, server_id).as_bytes()).unwrap();
                } else {
                    stream.write(b"ERROR: Key not found\n").unwrap();
                }
            } else {
                stream.write(b"ERROR: Invalid GET command\n").unwrap();
            }
        }
        "DEL" => {
            if parts.len() == 2 {
                let key = parts[1];
                let mut data = data.lock().unwrap();
                if data.remove(key).is_some() {
                    stream.write(format!("OK (from server {})\n", server_id).as_bytes()).unwrap();
                } else {
                    stream.write(b"ERROR: Key not found\n").unwrap();
                }
            } else {
                stream.write(b"ERROR: Invalid DEL command\n").unwrap();
            }
        }
        _ => {
            stream.write(b"ERROR: Invalid command\n").unwrap();
        }
    }

    stream.shutdown(Shutdown::Both).unwrap();
}

// simple consistent hashing function 
fn simple_hash(key: &str) -> usize {
    key.bytes().fold(0usize, |acc, b| acc.wrapping_add(b as usize)) % 3
}

// driver code
fn main() {
    let server1 = DHT::new(0);
    let server2 = DHT::new(1);
    let server3 = DHT::new(2);

    // thread handlers
    thread::spawn(move || {
        server1.start_server(8001);
    });

    thread::spawn(move || {
        server2.start_server(8002);
    });

    thread::spawn(move || {
        server3.start_server(8003);
    });

    // command-line client
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let input = line.unwrap();
        let parts: Vec<&str> = input.split(' ').collect();

        if parts.len() < 2 {
            println!("Invalid command");
            continue;
        }

        let command = parts[0];
        let key = parts[1];
        let hash = simple_hash(key);
        let port = match hash {
            0 => 8001,
            1 => 8002,
            2 => 8003,
            _ => unreachable!(),
        };

        // match command
        match command {
            "SET" | "GET" | "DEL" => {
                if let Ok(mut stream) = TcpStream::connect(format!("127.0.0.1:{}", port)) {
                    stream.write(input.as_bytes()).unwrap();
                    let mut response = String::new();
                    let mut reader = BufReader::new(&stream);
                    reader.read_line(&mut response).unwrap();
                    println!("{}", response.trim());
                } else {
                    println!("Cannot to connect to server");
                }
            }
            _ => println!("Invalid command"),
        }
    }
}
