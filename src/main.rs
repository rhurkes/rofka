extern crate rdkafka;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::message::Message;
use rdkafka::util::get_rdkafka_version;
use rocksdb::{ColumnFamilyDescriptor, DB, IteratorMode, Options};
use serde::Deserialize;
use std::str;
use std::time::Duration;

const ROCKSDB_PATH: &str = "message_store";
const IS_READ_MODE: bool = true;

#[derive(Deserialize, Debug, PartialEq)]
enum Status {
    APPROVED,
    INITIATED,
    UNPUBLISHED,
}

#[derive(Deserialize, Debug)]
struct ItemVersion {
    tcin: String,
    version: u32,
    source_system: String,
    source_timestamp: String,
    created_timestamp: String,
    status: Status,
}

#[derive(Deserialize, Debug)]
struct SmallItemVersion {
    tcin: String,
    version: u32,
    status: Status,
}

fn main() {
    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    
    let cf_opts = Options::default();
    let cf = ColumnFamilyDescriptor::new("tcin_version_status", cf_opts);
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    // let db = DB::open_default(ROCKSDB_PATH).unwrap();
    let db = DB::open_cf_descriptors(&db_opts, ROCKSDB_PATH, vec![cf]).unwrap();

    if !IS_READ_MODE {
        consume_and_store(&topics, brokers, &db);
    } else {
        // let iter = db.iterator(IteratorMode::Start);
        // for (key, _value) in iter {
        //     dbg!(str::from_utf8(&key).unwrap());
        // }

        let cf_handle =  db.cf_handle("tcin_version_status").unwrap();
        let cf_iter = db.iterator_cf(&cf_handle, IteratorMode::Start);
        for (key, value) in cf_iter {
            let siv: SmallItemVersion = serde_json::from_slice(&value).unwrap();
            if siv.status == Status::UNPUBLISHED {
                dbg!(str::from_utf8(&key).unwrap());
            }
        }
    }
}

fn consume_and_store(topic: &str, brokers: &str, db: &DB) {
    let mut config = ClientConfig::new();
    config.set("group.id", "test");
    config.set("bootstrap.servers", brokers);
    config.set("enable.auto.commit", "false");
    config.set("auto.offset.reset", "earliest");

    let topics = vec![topic];
    let consumer: BaseConsumer<DefaultConsumerContext> =
        config.create_with_context(DefaultConsumerContext).unwrap();
    consumer.subscribe(&topics).unwrap();

    let cf_handle =  db.cf_handle("tcin_version_status").unwrap();

    loop {
        let message_option = consumer.poll(Duration::from_millis(500));
        match message_option {
            None => (),
            Some(message_result) => match message_result {
                Ok(message) => {
                    let key = message.key();
                    let value = message.payload();

                    if key.is_some() && value.is_some() {
                        // let item_version: ItemVersion = serde_json::from_slice(value).unwrap();
                        // println!("{:#?}", item_version);
                        db.put(key.unwrap(), value.unwrap()).unwrap();
                        db.put_cf(cf_handle, key.unwrap(), value.unwrap()).unwrap();
                    }
                }
                Err(e) => println!("Kafka Error: {:?}", e),
            },
        }
    }
}
