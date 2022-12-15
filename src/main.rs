#[allow(unused_imports)]
use std::net::{SocketAddr, TcpListener, TcpStream};
use avro_rs::{Writer, Schema, types::{Record, Value}};
use std::thread::sleep;
use std::time::Duration;
use std::io::Write;


const RAW_SCHEMA: &str = r#"
        {
            "type": "record",
            "namespace": "com.geofen",
            "name": "geofen",
            "fields": [
                {"name": "timestamp", "type": "long"},
                {"name": "device_id", "type": "string"},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "altitude", "type": "double"}
            ]
        }
    "#;

fn main() {

    let schema = Schema::parse_str(RAW_SCHEMA).unwrap();
    

    let listner = TcpListener::bind("localhost:8000").unwrap();
    let (mut socket,_) = listner.accept().unwrap();
    let mut writer = Writer::new(&schema, &mut socket);

    loop {
        println!("Writing");
        let mut record = Record::new(&schema).unwrap();

        record.put("timestamp", 1234567890i64);
        record.put("device_id", "device1");
        record.put("latitude", 12.345678f64);
        record.put("longitude", 98.765432f64);
        record.put("altitude", 123.456789f64);

        // print!("{:?}", record);
        writer.append(record).unwrap();
        let _encoded = writer.flush().unwrap();
 //       print!("{:?}", encoded);
        sleep(Duration::from_secs(2)); 
    }
}
