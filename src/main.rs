#![warn(unused_extern_crates)]

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate paho_mqtt as mqtt;

extern crate serde_json;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate strum_macros;

extern crate rand;

use rand::prelude::{thread_rng, Rng};
const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
abcdefghijklmnopqrstuvwxyz\
0123456789";
const USERNAME_LEN: usize = 10;

use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Write};

const COMMAND_LIST: [&str; 2] = ["help", "exit"];
const MQTT_BROKER_IP: &str = "127.0.0.1";
const MQTT_BROKER_PORT: &str = "1883";
const MQTT_BROKER_UNREGISTERED_USER: &str = "unregistered_node";
const MQTT_BROKER_UNREGISTERED_PASS: &str = "unregistered";

const TOPIC_REGISTERED: &str = "registered";
const TOPIC_UNREGISTERED: &str = "unregistered";

const PATH_CLIENT: &str = "client.txt";

// Must be a valid vector of 'element', if its not then the master server will reject it.
const ELEMENTS: &str = "[{\"address\":\"0xtest_address\", \"element_type\":\"BasicSwitch\"},{\"address\":\"0xtest_address1\", \"element_type\":\"BasicSwitch\"}]";

#[derive(Debug, Serialize, Deserialize)]
pub struct Command {
    pub command: CommandType,
    pub data: String,
}

#[derive(Debug, Serialize, Deserialize, ToString, PartialEq)]
pub enum CommandType {
    Announce,           // Command type from BlackBox
    AnnounceOffline,    // Notifying BlackBox that the node is offline
    AnnounceOnline,     // Notifying BlackBox that the node is online
    ImplementCreds,     // Command type from Blackbox (received when creds are being sent)
    UnregisterNotify,         // Command type from BlackBox (received when unregistered)
    ElementSummary,     // When sending the element summary list of this node
    SetElementState,    // Command type from BlackBox
    UpdateElementState, // Notifying BlackBox about state change (possibly external)
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

    let mut test_address: bool = false;

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
        clientid = generate_username();
        clientpassword = MQTT_BROKER_UNREGISTERED_PASS.to_string();
        clientusername = MQTT_BROKER_UNREGISTERED_USER.to_string();
    }

    info!("USERNAME: {}", clientusername);
    info!("PASSWORD: {}", clientpassword);
    info!("CLIENT ID: {}", clientid);

    /*Try to find file client.conf 
    If found, read the client id and registered credentials from it
    Else, try to log in as "unregistered" with randomly generated client id and sub to those topics */

    let broker_node_path;

    if clientusername == MQTT_BROKER_UNREGISTERED_USER {
        broker_node_path = format!("unregistered/{}", clientid);
    } else {
        broker_node_path = format!("registered/{}", clientid);
    }

    let _mqtt_broker_addr: &str =
        &format!("tcp://{}:{}", MQTT_BROKER_IP, MQTT_BROKER_PORT).to_string();

    let mut cli = mqtt::AsyncClient::new((_mqtt_broker_addr, &*clientid)).unwrap_or_else(|e| {
        error!("Error creating mqtt client: {:?}", e);
        std::process::exit(0);
    });

    // Set a closure to be called whenever the client loses the connection.
    // It will attempt to reconnect, and set up function callbacks to keep
    // retrying until the connection is re-established.
    cli.set_connection_lost_callback(|cli: &mut mqtt::AsyncClient| {
        error!("Connection lost. Attempting reconnect.");
        std::thread::sleep(std::time::Duration::from_millis(2500));
        cli.reconnect_with_callbacks(on_mqtt_connect_success, on_mqtt_connect_failure);
    });

    let _clientusername = clientusername.to_owned();

    // Attach a closure to the client to receive callback
    // on incoming messages.
    cli.set_message_callback(move |_cli, msg| {
        if let Some(msg) = msg {
            let topic = msg.topic().split("/");
            let payload_str = msg.payload_str();

            let topic_split: Vec<&str> = topic.collect();

            if topic_split.len() == 1 {
                if topic_split[0] == "unregistered" {
                    match serde_json::from_str(&payload_str) {
                        Ok(result) => {
                            let cmd: Command = result;

                            if cmd.command.to_string()
                                == CommandType::Announce.to_string()
                            {
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
                                    serde_json::to_string(&new_command(CommandType::AnnounceOnline, "")).unwrap(),
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

                                if cmd.command.to_string()
                                    == CommandType::ImplementCreds.to_string()
                                {
                                    let payload = cmd.data.split(",");

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
                                    let payload = cmd.data.split(",");

                                    let args: Vec<&str> = payload.collect();

                                    if topic_split.len() > 1 && args[0] == "0xtest_address" {
                                        test_address = if args[1] == "1" {true} else {false};
                                        info!("test_element set to: {}", test_address);

                                        let payload = format!("{},{}", "0xtest_address", if test_address == false {"0"} else {"1"});
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
        serde_json::to_string(&new_command(CommandType::AnnounceOffline, "")).unwrap(),
        1,
    );

    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .keep_alive_interval(std::time::Duration::from_secs(20))
        .mqtt_version(mqtt::MQTT_VERSION_3_1_1)
        .clean_session(true)
        .will_message(lwt)
        //.ssl()
        .user_name(clientusername)
        .password(&clientpassword)
        .finalize();

    // Make the connection to the broker
    info!("Connecting to MQTT broker...");
    cli.connect_with_callbacks(conn_opts, on_mqtt_connect_success, on_mqtt_connect_failure);
    /**/
    'commands: loop {
        /*print!("> ");
        io::stdout().flush().ok().unwrap();*/

        let mut command: String = String::new();
        io::stdin()
            .read_line(&mut command)
            .expect("Error reading command.");

        match command.trim().as_ref() {
            "help" => {
                print!("Available Commands: ");

                let _iter = COMMAND_LIST.iter();
                let _num_of_cmds = _iter.len();

                for comm in _iter {
                    print!("{} ", comm)
                }
                println!("");
            }
            "exit" => {
                print!("Are you sure you want to stop Node Emulator? [y]es | [n]o : ");
                io::stdout().flush().ok().unwrap();

                let mut conf: String = String::new();
                io::stdin()
                    .read_line(&mut conf)
                    .expect("Error reading confirmation.");

                if conf.chars().next().unwrap() == 'y' {
                    println!("Node Emulator shutdown");
                    break;
                } else if conf.chars().next().unwrap() == 'n' {
                    continue;
                }
            }
            _ => println!("Unknown command. Type 'help' for a list of commands."),
        }
    }
}

pub fn generate_username() -> String {
    let mut rng = thread_rng();
    let username: Option<String> = (0..USERNAME_LEN)
        .map(|_| Some(*rng.choose(CHARSET)? as char))
        .collect();
    username.unwrap()
}

fn send_element_summary(_cli: &mqtt::AsyncClient, clientid: &str) {
    //println!("Client ID: {}", clientid);
    let msg = mqtt::MessageBuilder::new()
        .topic("unregistered/".to_owned() + clientid)
        .payload(serde_json::to_string(&new_command(
                CommandType::ElementSummary,
                ELEMENTS,
            )).unwrap())
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
    let mut clientpassword = String::new();
    let mut clientusername = String::new();

    let input = File::open(PATH_CLIENT).unwrap();
    let buffered = BufReader::new(input);

    for line in buffered.lines() {
        let matchcase = String::from(line.unwrap());

        match matchcase.chars().next() {
            Some('1') => {
                // Username
                clientusername = matchcase[1..].trim().to_string();
            }
            Some('2') => {
                // Password
                clientpassword = matchcase[1..].trim().to_string();
            }
            Some(_) => {
                // Something else
                error!("WTF: {}", matchcase);
            }
            None => {
                warn!("Nothing was found in client file...");
            }
        }
    }
    (clientusername, clientpassword)
}

fn save_client_file(client_identifier: &str, password: &str) {
    let client_file = "1 ";

    let client_file = format!("{}{}", client_file, client_identifier);

    let client_file_1 = "
2 ";
    let client_file = format!("{}{}", client_file, client_file_1);

    let client_file = format!("{}{}", client_file, password);

    let mut file = File::create(PATH_CLIENT).unwrap();
    file.write_all(&format!("{}", client_file).as_bytes())
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
            serde_json::to_string(&new_command(CommandType::AnnounceOnline, "")).unwrap(),
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
        let client_id = cli.client_id.to_str().unwrap();

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
