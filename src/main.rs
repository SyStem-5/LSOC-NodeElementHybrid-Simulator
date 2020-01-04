#![warn(unused_extern_crates)]

#[macro_use]
extern crate log;
use env_logger;

extern crate paho_mqtt as mqtt;

use serde_json;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate strum_macros;

use rand;

use crate::rand::prelude::SliceRandom;
use rand::prelude::{thread_rng};
const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
abcdefghijklmnopqrstuvwxyz\
0123456789";
const USERNAME_LEN: usize = 10;

use std::fs::File;
use std::io::{BufRead, BufReader, Write};

const MQTT_BROKER_IP: &str = "127.0.0.1";
const MQTT_BROKER_PORT: &str = "8883";
const MQTT_BROKER_UNREGISTERED_USER: &str = "unregistered_node";
const MQTT_BROKER_UNREGISTERED_PASS: &str = "unregistered";

const TOPIC_REGISTERED: &str = "registered";
const TOPIC_UNREGISTERED: &str = "unregistered";

const PATH_CLIENT: &str = "client.txt";

// Must be a valid vector of 'element', if its not then the master server will reject it.
const ELEMENTS: &str = "[{\"address\":\"0xtest_address\", \"element_type\":\"BasicSwitch\"},{\"address\":\"0xtest_address1\", \"element_type\":\"BasicSwitch\"},{\"address\":\"0xtest_address2\", \"element_type\":\"Thermostat\"},{\"address\":\"0xtest_address3\", \"element_type\":\"DHT11\"}]";

#[derive(Debug, Serialize, Deserialize)]
pub struct Command {
    pub command: CommandType,
    pub data: String,
}

#[derive(Debug, Serialize, Deserialize, ToString, PartialEq)]
pub enum CommandType {
    Announce,           // Command type from BlackBox
    AnnounceState,      // Notifying BlackBox about the node state
    ImplementCreds,     // Command type from Blackbox (received when creds are being sent)
    UnregisterNotify,   // Command type from BlackBox (received when unregistered)
    ElementSummary,     // When sending the element summary list of this node
    SetElementState,    // Command type from BlackBox
    UpdateElementState, // Notifying BlackBox about state change (possibly external)
    RestartDevice,      // Command type from BlackBox (we need to restart)
}

fn new_command(command: CommandType, data: &str) -> Command {
    Command {
        command,
        data: data.to_string(),
    }
}

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let test_address = std::sync::Arc::from(std::sync::atomic::AtomicBool::new(false));
    let test_address1 = std::sync::Arc::from(std::sync::atomic::AtomicBool::new(false));
    let test_address2 = std::sync::Arc::from(std::sync::Mutex::new(0));

    let clientid;
    let clientpassword;
    let clientusername;

    info!("Node emulator startup.");

    if std::path::Path::new(PATH_CLIENT).exists() {
        let creds = get_client_file();
        clientid = (creds.0).to_owned();
        clientpassword = creds.1;
        clientusername = creds.0;
    } else {
        clientid = generate_clientid();
        clientpassword = MQTT_BROKER_UNREGISTERED_PASS.to_string();
        clientusername = MQTT_BROKER_UNREGISTERED_USER.to_string();
    }

    info!("USERNAME: {}", clientusername);
    info!("PASSWORD: {}", clientpassword);
    info!("CLIENT ID: {}", clientid);

    /*
    Try to find file client.conf
    If found, read the client id and registered credentials from it
    Else, try to log in as "unregistered" with randomly generated client id and sub to those topics
    */

    let broker_node_path = if clientusername == MQTT_BROKER_UNREGISTERED_USER {
        format!("unregistered/{}", clientid)
    } else {
        format!("registered/{}", clientid)
    };

    let _mqtt_broker_addr: &str =
        &format!("ssl://{}:{}", MQTT_BROKER_IP, MQTT_BROKER_PORT);

    let mut cli = mqtt::AsyncClient::new((_mqtt_broker_addr, &*clientid)).unwrap_or_else(|e| {
        error!("Error creating mqtt client: {:?}", e);
        std::process::exit(0);
    });

    // Set a closure to be called whenever the client loses the connection.
    // It will attempt to reconnect, and set up function callbacks to keep
    // retrying until the connection is re-established.
    cli.set_connection_lost_callback(|cli: &mqtt::AsyncClient| {
        error!("Connection lost. Attempting reconnect.");
        std::thread::sleep(std::time::Duration::from_millis(2500));
        cli.reconnect_with_callbacks(on_mqtt_connect_success, on_mqtt_connect_failure);
    });

    let client_id_clone = clientid.to_owned();

    let send_done_restarting = std::sync::Arc::from(std::sync::atomic::AtomicBool::from(false));

    let send_node_restarting_clone = send_done_restarting.clone();

    let test_address_clone = test_address.clone();
    let test_address_clone1 = test_address1.clone();
    let test_address_clone2 = test_address2.clone();

    let _clientusername = clientusername.to_owned();

    // Attach a closure to the client to receive callback
    // on incoming messages.
    cli.set_message_callback(move |_cli, msg| {
        if let Some(msg) = msg {
            let topic = msg.topic().split('/');
            let payload_str = msg.payload_str();

            let topic_split: Vec<&str> = topic.collect();

            if topic_split.len() == 1 {
                if topic_split[0] == "unregistered" {
                    match serde_json::from_str(&payload_str) {
                        Ok(result) => {
                            let cmd: Command = result;

                            if cmd.command.to_string() == CommandType::Announce.to_string() {
                                info!("Responding to 'Announce' request...");
                                // Send a list of elements available
                                send_element_summary(_cli, &clientid);
                            } else {
                                warn!(
                                    "Got weird command from unregistered topic: {:?} With Data: {}",
                                    cmd.command, cmd.data
                                );
                            }
                        }
                        Err(e) => {
                            error!("Failed to convert command payload from BlackBox 'unregistered' =1. {}", e);
                        }
                    }
                } else {
                    // If the topic is registered topic
                    match serde_json::from_str(&payload_str) {
                        Ok(result) => {
                            let cmd: Command = result;

                            if cmd.command == CommandType::Announce {
                                let msg = mqtt::Message::new(
                                    format!("registered/{}", _clientusername),
                                    serde_json::to_string(&new_command(CommandType::AnnounceState, "true")).unwrap(),
                                    1,
                                );
                                let _tok = _cli.publish(msg);
                                info!("SENDING ANNOUNCE RESPONSE");
                            } else {
                                warn!(
                                    "Got weird command from registered topic: {:?} With Data: {}",
                                    cmd.command, cmd.data
                                );
                            }
                        }
                        Err(e) => {
                            error!("Failed to convert command payload from BlackBox registered topic. {} Payload: {}", e, payload_str);
                        }
                    }
                }
            } else if topic_split.len() > 1 {
                if topic_split[0] == "unregistered" {
                    if topic_split[1] == clientid {
                        match serde_json::from_str(&payload_str) {
                            Ok(result) => {
                                let cmd: Command = result;

                                if cmd.command.to_string()  == CommandType::ImplementCreds.to_string() {
                                    let payload = cmd.data.split(':');

                                    let creds: Vec<&str> = payload.collect();
                                    if creds.len() > 1 {
                                        info!("Saving credentials: {:?}", creds);

                                        save_client_file(creds[0], creds[1]);
                                    }
                                } else {
                                    warn!(
                                        "Got weird command from self.unregistered topic: {:?} With Data: {}",
                                        cmd.command, cmd.data
                                    );
                                }
                            }
                            Err(e) => {
                                error!("Failed to convert command payload from BlackBox self.unregistered topic. {} Payload: {}", e, payload_str);
                            }
                        }
                    }
                } else {
                    // If the topic is registered topic and client id matches ours
                    match serde_json::from_str(&payload_str) {
                        Ok(result) => {
                            let cmd: Command = result;

                            match cmd.command {
                                CommandType::SetElementState => {
                                    dbg!(&cmd.data);

                                    #[derive(Deserialize)]
                                    struct ElementData {
                                        id: String,
                                        data: String
                                    }

                                    match serde_json::from_str::<ElementData>(&cmd.data) {
                                        Ok(mut parsed) => {
                                            match parsed.id.as_ref() {
                                                // BasicSwitch
                                                "0xtest_address" => {
                                                    if parsed.data == "false" {
                                                        test_address_clone.store(false, std::sync::atomic::Ordering::Relaxed);
                                                    } else {
                                                        test_address_clone.store(true, std::sync::atomic::Ordering::Relaxed);
                                                    }
                                                }
                                                // BasicSwitch
                                                "0xtest_address1" => {
                                                    if parsed.data == "false" {
                                                        test_address_clone1.store(false, std::sync::atomic::Ordering::Relaxed);
                                                    } else {
                                                        test_address_clone1.store(true, std::sync::atomic::Ordering::Relaxed);
                                                    }
                                                }
                                                // Thermostat
                                                "0xtest_address2" => {
                                                    if let Ok(mut lock) = test_address_clone2.lock() {
                                                        *lock = if &parsed.data == "+" {
                                                            *lock + 1
                                                        } else {
                                                            *lock - 1
                                                        };

                                                        parsed.data = lock.clone().to_string();
                                                    }
                                                }
                                                _ => {}
                                            }

                                            let payload = format!("{}::{}", &parsed.id, &parsed.data.replace("\'", "\""));
                                            let msg = mqtt::MessageBuilder::new()
                                                .topic("registered/".to_owned() + &clientid)
                                                .payload(
                                                    serde_json::to_string(&new_command(
                                                        CommandType::UpdateElementState,
                                                        &payload,
                                                    )).unwrap()
                                                )
                                                .qos(1)
                                                .finalize();
                                            _cli.publish(msg);
                                        }
                                        Err(_) => error!("Could not parse element data SetElementState")
                                    }
                                }
                                CommandType::RestartDevice => {
                                    send_node_restarting_clone.store(true, std::sync::atomic::Ordering::Relaxed);

                                    let msg = mqtt::Message::new(
                                        ["registered/", &clientid].concat(),
                                            serde_json::to_string(&new_command(
                                            CommandType::AnnounceState,
                                            "restarting",
                                        )).unwrap(), 2);
                                    _cli.publish(msg);

                                    warn!("SIMULATING NODE RESTART...");

                                }
                                CommandType::UnregisterNotify => {
                                    info!("We have been unregistered.");
                                }
                                _ => warn!("Got weird command from self.registered topic: {:?} With Data: {}", cmd.command, cmd.data)
                            }
                        }
                        Err(e) => {
                            error!("Failed to convert command payload from BlackBox self.registered topic. {} Payload: {}", e, payload_str);
                        }
                    }
                }
            }
        }
    });

    // Define the set of options for the connection
    let lwt = mqtt::Message::new(
        broker_node_path,
        serde_json::to_string(&new_command(CommandType::AnnounceState, "false")).unwrap(),
        1,
    );

    let ssl = mqtt::SslOptionsBuilder::new()
        .trust_store("/etc/mosquitto/ca.crt")
        .finalize();

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(std::time::Duration::from_secs(20))
        .mqtt_version(mqtt::MQTT_VERSION_3_1_1)
        .clean_session(true)
        .will_message(lwt)
        .ssl_options(ssl)
        .user_name(clientusername.to_owned())
        .password(&clientpassword)
        .finalize();

    // Make the connection to the broker
    info!("Connecting to MQTT broker...");
    cli.connect_with_callbacks(conn_opts, on_mqtt_connect_success, on_mqtt_connect_failure);

    use rand::Rng;
    let mut rng = rand::thread_rng();

    loop {

        std::thread::sleep(std::time::Duration::from_secs(5));

        if clientusername != MQTT_BROKER_UNREGISTERED_USER {
            info!("-----------------------------------------------");
            warn!("ELEM0: {}", test_address.load(std::sync::atomic::Ordering::Relaxed));
            warn!("ELEM1: {}", test_address1.load(std::sync::atomic::Ordering::Relaxed));
            warn!("ELEM2: {}", test_address2.as_ref().lock().unwrap());
            let msg = mqtt::Message::new(
                ["registered/", &client_id_clone].concat(),
                serde_json::to_string(&new_command(
                    CommandType::UpdateElementState,
                    &format!("0xtest_address3::{{\"temp\": \"{}\", \"hum\": \"{}\"}}", rng.gen_range(20, 23), rng.gen_range(70, 78)),
                )).unwrap(),
                1
            );
            cli.publish(msg);
        }

        if send_done_restarting.load(std::sync::atomic::Ordering::Relaxed) && clientusername != MQTT_BROKER_UNREGISTERED_USER {
            let msg = mqtt::Message::new(
                ["registered/", &clientusername].concat(),
                    serde_json::to_string(&new_command(
                    CommandType::AnnounceState,
                    "true",
                )).unwrap(), 2);
            cli.publish(msg);

            send_done_restarting.store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

pub fn generate_clientid() -> String {
    let mut rng = thread_rng();
    let username: Option<String> = (0..USERNAME_LEN)
        .map(|_| Some(*CHARSET.choose(&mut rng)? as char))
        .collect();
    username.unwrap()
}

fn send_element_summary(_cli: &mqtt::AsyncClient, clientid: &str) {
    //println!("Client ID: {}", clientid);
    let payload = serde_json::to_string(&new_command(
                CommandType::ElementSummary,
                ELEMENTS,
            )).unwrap();

    let msg = mqtt::MessageBuilder::new()
        .topic("unregistered/".to_owned() + clientid)
        .payload(payload)
        .qos(1)
        .finalize();

    _cli.publish(msg);
}

/**
 * Read client credentials from disk
 * * `Touple.0` - client username/clientid
 * * `Touple.1` - client password
 */
fn get_client_file() -> (String, String) {
    let clientusername;
    let clientpassword;

    let input = File::open(PATH_CLIENT).unwrap();
    let mut buffered = BufReader::new(input);

    let mut line = String::new();
    buffered.read_line(&mut line).expect("Could not read client credentials.");

    let data: Vec<&str> = line.split(':').collect();
    clientusername = data[0].to_owned();
    clientpassword = data[1].to_owned();

    (clientusername, clientpassword)
}

fn save_client_file(client_identifier: &str, password: &str) {
    let contents = [client_identifier, ":", password].concat();

    let mut file = File::create(PATH_CLIENT).unwrap();
    file.write_all(&contents.as_bytes())
        .unwrap();

    info!("Client file saved. Restart emulator.");
    std::process::exit(0);
}

fn on_mqtt_connect_success(cli: &mqtt::AsyncClient, _msgid: u16) {
    info!("Connection succeeded.");
    // Subscribe to the desired topic(s).

    if std::path::Path::new(PATH_CLIENT).exists() {
        let creds = get_client_file();

        let registered_topic = format!("registered/{}", creds.0);

        // Send the 'online' payload before subscribing so we avoid processing our own command
        let msg = mqtt::Message::new(
            registered_topic.to_owned(),
            serde_json::to_string(&new_command(CommandType::AnnounceState, "true")).unwrap(),
            1,
        );
        let _tok = cli.publish(msg);

        cli.subscribe(registered_topic.to_owned(), 1);
        cli.subscribe(TOPIC_REGISTERED, 1);

        info!(
            "Subscribing to registered topics. Self topic: {}",
            registered_topic
        );
    } else {
        let client_id = cli.inner.client_id.to_str().unwrap();

        // Subscribe to the randomly generated clientid
        let unregistered_topic = format!("unregistered/{}", client_id);

        cli.subscribe(unregistered_topic.clone(), 1);
        cli.subscribe(TOPIC_UNREGISTERED, 1);

        send_element_summary(cli, client_id);

        info!("Subscribing to topic: {}", unregistered_topic);
    }
}

fn on_mqtt_connect_failure(cli: &mqtt::AsyncClient, _msgid: u16, rc: i32) {
    warn!("Connection attempt failed with error code {}.\n", rc);

    std::thread::sleep(std::time::Duration::from_millis(2500));
    cli.reconnect_with_callbacks(on_mqtt_connect_success, on_mqtt_connect_failure);
}
