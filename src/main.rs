use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};
use std::env;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Deserialize, Debug)]
struct Instrument {
    instrument_name: String,
}

#[derive(Deserialize, Debug)]
struct ApiResponse {
    result: Vec<Instrument>,
}

async fn get_instrument_names(currency: &str) -> Result<Vec<String>, reqwest::Error> {
    let url = "https://www.deribit.com/api/v2/public/get_instruments";
    let params = [
        ("currency", currency),
        ("kind", "option"),
        ("expired", "false"),
    ];

    let client = Client::new();
    let response = client.get(url).query(&params).send().await?;
    let data = response.json::<ApiResponse>().await?;
    let instruments = data
        .result
        .into_iter()
        .map(|item| item.instrument_name)
        .collect();
    Ok(instruments)
}

fn deep_merge(a: &mut Value, b: &Value) {
    match (a, b) {
        (Value::Object(a_map), Value::Object(b_map)) => {
            for (k, v) in b_map {
                deep_merge(a_map.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (a, b) => {
            *a = b.clone();
        }
    }
}

async fn subscribe_to_instruments(
    instrument_names: Vec<String>,
    debug_mode: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let uri = "wss://www.deribit.com/ws/api/v2";
    let (ws_stream, _) = connect_async(uri).await.expect("Failed to connect");
    let (mut write, mut read) = ws_stream.split();

    println!("Connected to WebSocket.");

    // Batch the instruments into groups of 50
    for (i, batch) in instrument_names.chunks(50).enumerate() {
        let channels: Vec<String> = batch
            .iter()
            .map(|instrument| format!("incremental_ticker.{}", instrument))
            .collect();

        let subscription_message = json!({
            "jsonrpc": "2.0",
            "id": 1011,
            "method": "public/subscribe",
            "params": {
                "channels": channels
            }
        });

        write
            .send(Message::Text(subscription_message.to_string()))
            .await?;
        println!("Subscribed to batch {}", i + 1);
    }

    // Connect to Redis
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = redis_client.get_async_connection().await?;

    // Listen for messages
    while let Some(message) = read.next().await {
        let msg = if let Ok(m) = message {
            m
        } else {
            println!("WebSocket read error.");
            continue;
        };

        if let Message::Text(text) = msg {
            if let Ok(rpc_data) = serde_json::from_str::<Value>(&text) {
                if let Some(params) = rpc_data.get("params") {
                    if let Some(data) = params.get("data") {
                        if let (Some(type_), Some(instrument_name)) = (
                            data.get("type").and_then(Value::as_str),
                            data.get("instrument_name").and_then(Value::as_str),
                        ) {
                            let redis_key = format!("deribit:{}", instrument_name);

                            if type_ == "snapshot" || type_ == "change" {
                                let existing_data_json: Option<String> =
                                    con.get(&redis_key).await?;

                                if let Some(existing_json) = existing_data_json {
                                    let mut existing_data: Value =
                                        serde_json::from_str(&existing_json)?;
                                    deep_merge(&mut existing_data, data);
                                    let updated_data_str = serde_json::to_string(&existing_data)?;
                                    let _: () = con.set(&redis_key, updated_data_str).await?;
                                    if debug_mode {
                                        println!(
                                            "Updated existing data for {} with key: {}",
                                            type_, redis_key
                                        );
                                    }
                                } else {
                                    let new_data_str = serde_json::to_string(data)?;
                                    let _: () = con.set(&redis_key, new_data_str).await?;
                                    if debug_mode {
                                        println!(
                                            "Stored new data for {} with key: {}",
                                            type_, redis_key
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let debug_mode = args.iter().any(|arg| arg == "--debug");
    let currency = args
        .iter()
        .skip(1)
        .find(|arg| !arg.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| "BTC".to_string());

    println!("Fetching instruments for currency: {}", &currency);

    let instrument_names = get_instrument_names(&currency).await?;
    println!("Total instruments: {}", instrument_names.len());

    subscribe_to_instruments(instrument_names, debug_mode).await?;

    Ok(())
}
