/*
MIT License

Copyright (c) 2022-2026 The Trzsz Authors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use crate::escape;
use crate::filter::{TrzszOptions, TrzszTrigger};
use crate::transfer::{K_PROTOCOL_VERSION, TransferAction, TransferConfig};
use crate::version::TrzszVersion;

const TRIGGER_MARKER: &[u8] = b"::TRZSZ:TRANSFER:";
const MAX_TRIGGER_LINE: usize = 512;
const MAX_PROTOCOL_LINE: usize = 1024 * 1024;
const MAX_HANDSHAKE_BUFFER: usize = 16 * 1024 * 1024;
const READ_BUFFER_SIZE: usize = 32 * 1024;
const RELAY_POLL_INTERVAL: Duration = Duration::from_millis(50);
const TUNNEL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayStatus {
    Standby,
    Handshaking,
    Transferring,
}

/// Connector used to open the server-side TCP connection for tunnel transfers.
pub type TunnelConnector = dyn Fn(i32) -> io::Result<TcpStream> + Send + Sync + 'static;

type StateCallback = dyn Fn(bool) + Send + Sync + 'static;
type SharedWriter = Arc<Mutex<Option<Box<dyn Write + Send>>>>;
type SharedReader = Mutex<Option<Box<dyn Read + Send>>>;

/// Relay endpoint for forwarding a trzsz client through an intermediate host.
///
/// `run` is blocking and single-use. `close` requests graceful shutdown. Generic
/// readers cannot be interrupted by Rust; hosts with blocking readers can install
/// shutdown callbacks to wake them when closing the relay.
pub struct TrzszRelay {
    client_in: SharedReader,
    client_out: SharedWriter,
    server_in: SharedWriter,
    server_out: SharedReader,
    options: TrzszOptions,
    closed: Arc<AtomicBool>,
    running: AtomicBool,
    tunnel_connector: Mutex<Option<Arc<TunnelConnector>>>,
    state_callback: Mutex<Option<Arc<StateCallback>>>,
    client_input_shutdown: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    server_output_shutdown: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    tunnel_listener: Mutex<Option<Arc<TcpListener>>>,
    listener_generation: Arc<AtomicUsize>,
}

struct Runtime {
    status: RelayStatus,
    trigger_detector: Vec<u8>,
    trigger: Option<RelayTrigger>,
    client_buffer: Vec<u8>,
    server_buffer: Vec<u8>,
    action: Option<TransferAction>,
    client_is_windows: bool,
    tunnel_active: bool,
    tunnel: Option<TunnelStreams>,
    client_end_scan: Vec<u8>,
    server_end_scan: Vec<u8>,
}

impl Default for Runtime {
    fn default() -> Self {
        Runtime {
            status: RelayStatus::Standby,
            trigger_detector: Vec::new(),
            trigger: None,
            client_buffer: Vec::new(),
            server_buffer: Vec::new(),
            action: None,
            client_is_windows: false,
            tunnel_active: false,
            tunnel: None,
            client_end_scan: Vec::new(),
            server_end_scan: Vec::new(),
        }
    }
}

struct RelayTrigger {
    parsed: TrzszTrigger,
    unique_id: String,
    server_port: i32,
    relay_port: i32,
}

struct TunnelStreams {
    client: TcpStream,
    server: TcpStream,
}

enum RelayEvent {
    ClientData(Vec<u8>),
    ClientEof,
    ClientError(io::Error),
    ServerData(Vec<u8>),
    ServerEof,
    ServerError(io::Error),
    TunnelReady {
        client: TcpStream,
        server: TcpStream,
    },
    TunnelClientData(Vec<u8>),
    TunnelClientEof,
    TunnelClientError(io::Error),
    TunnelServerData(Vec<u8>),
    TunnelServerEof,
    TunnelServerError(io::Error),
}

impl TrzszRelay {
    /// Construct a relay around the four client/server I/O endpoints.
    ///
    /// No threads are started until [`run`](Self::run) is called.
    pub fn new(
        client_in: Box<dyn Read + Send>,
        client_out: Box<dyn Write + Send>,
        server_in: Box<dyn Write + Send>,
        server_out: Box<dyn Read + Send>,
        options: TrzszOptions,
    ) -> Self {
        TrzszRelay {
            client_in: Mutex::new(Some(client_in)),
            client_out: Arc::new(Mutex::new(Some(client_out))),
            server_in: Arc::new(Mutex::new(Some(server_in))),
            server_out: Mutex::new(Some(server_out)),
            options,
            closed: Arc::new(AtomicBool::new(false)),
            running: AtomicBool::new(false),
            tunnel_connector: Mutex::new(None),
            state_callback: Mutex::new(None),
            client_input_shutdown: Mutex::new(None),
            server_output_shutdown: Mutex::new(None),
            tunnel_listener: Mutex::new(None),
            listener_generation: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Set the connector used to open a server-side tunnel connection.
    pub fn set_tunnel_connector<F>(&self, connector: F)
    where
        F: Fn(i32) -> io::Result<TcpStream> + Send + Sync + 'static,
    {
        *lock(&self.tunnel_connector) = Some(Arc::new(connector));
    }

    /// Disable tunnel support and stop advertising relay tunnel ports.
    pub fn clear_tunnel_connector(&self) {
        *lock(&self.tunnel_connector) = None;
        self.stop_tunnel_listener();
    }

    /// Notify the host when relayed file transfer starts or finishes.
    pub fn set_transfer_state_callback<F>(&self, callback: F)
    where
        F: Fn(bool) + Send + Sync + 'static,
    {
        *lock(&self.state_callback) = Some(Arc::new(callback));
    }

    /// Remove the currently installed transfer-state callback.
    pub fn clear_transfer_state_callback(&self) {
        *lock(&self.state_callback) = None;
    }

    /// Install callbacks which wake blocking client-input/server-output readers
    /// when `close` is called.
    pub fn set_shutdown_handlers(
        &self,
        client_input: Option<Arc<dyn Fn() + Send + Sync>>,
        server_output: Option<Arc<dyn Fn() + Send + Sync>>,
    ) {
        *lock(&self.client_input_shutdown) = client_input;
        *lock(&self.server_output_shutdown) = server_output;
    }

    /// Request graceful relay shutdown and close any active tunnel listener.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.stop_tunnel_listener();
        let client_input_shutdown = lock(&self.client_input_shutdown).clone();
        if let Some(callback) = client_input_shutdown {
            callback();
        }
        let server_output_shutdown = lock(&self.server_output_shutdown).clone();
        if let Some(callback) = server_output_shutdown {
            callback();
        }
    }

    /// Run the relay until both ordinary streams reach EOF or `close` is called.
    pub fn run(&self) -> io::Result<()> {
        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "relay run may only be called once",
            ));
        }
        if self.closed.load(Ordering::SeqCst) {
            self.finish_runtime(&mut Runtime::default());
            return Ok(());
        }

        let client_in = lock(&self.client_in).take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "client input already consumed")
        })?;
        let server_out = lock(&self.server_out).take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "server output already consumed",
            )
        })?;
        let (sender, receiver) = mpsc::sync_channel(64);
        if let Err(error) = spawn_reader(client_in, sender.clone(), self.closed.clone(), true) {
            self.close();
            return Err(error);
        }
        if let Err(error) = spawn_reader(server_out, sender.clone(), self.closed.clone(), false) {
            self.close();
            return Err(error);
        }

        let mut runtime = Runtime::default();
        let result = self.run_events(receiver, sender, &mut runtime);
        self.finish_runtime(&mut runtime);
        result
    }

    fn run_events(
        &self,
        receiver: Receiver<RelayEvent>,
        sender: SyncSender<RelayEvent>,
        runtime: &mut Runtime,
    ) -> io::Result<()> {
        let mut client_eof = false;
        let mut server_eof = false;
        let mut first_error = None;

        while !self.closed.load(Ordering::SeqCst) {
            if client_eof && server_eof {
                break;
            }
            let event = match receiver.recv_timeout(RELAY_POLL_INTERVAL) {
                Ok(event) => event,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            match event {
                RelayEvent::ClientData(bytes) => {
                    self.process_client_data(runtime, &bytes)?;
                }
                RelayEvent::ClientEof => {
                    client_eof = true;
                    close_writer(&self.server_in);
                }
                RelayEvent::ClientError(error) => {
                    first_error.get_or_insert(error);
                    client_eof = true;
                    close_writer(&self.server_in);
                }
                RelayEvent::ServerData(bytes) => {
                    self.process_server_data(runtime, &bytes, &sender)?;
                }
                RelayEvent::ServerEof => {
                    server_eof = true;
                    if runtime.status == RelayStatus::Handshaking {
                        self.fail_handshake(runtime, "Relay server closed during handshake")?;
                    } else if runtime.status == RelayStatus::Transferring {
                        self.end_transfer(runtime);
                    }
                    let trailing = self.finish_trigger_detection(runtime);
                    if !trailing.is_empty() {
                        write_shared(&self.client_out, &trailing)?;
                    }
                    close_writer(&self.client_out);
                }
                RelayEvent::ServerError(error) => {
                    first_error.get_or_insert(error);
                    server_eof = true;
                    close_writer(&self.client_out);
                }
                RelayEvent::TunnelReady { client, server } => {
                    self.install_tunnel(runtime, client, server, sender.clone())?;
                }
                RelayEvent::TunnelClientData(bytes) => {
                    self.process_tunnel_client_data(runtime, &bytes)?;
                }
                RelayEvent::TunnelServerData(bytes) => {
                    self.process_tunnel_server_data(runtime, &bytes)?;
                }
                RelayEvent::TunnelClientEof | RelayEvent::TunnelServerEof => {
                    self.end_transfer(runtime);
                }
                RelayEvent::TunnelClientError(error) | RelayEvent::TunnelServerError(error) => {
                    first_error.get_or_insert(error);
                    self.end_transfer(runtime);
                }
            }
        }
        if self.closed.load(Ordering::SeqCst) {
            Ok(())
        } else if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn process_client_data(&self, runtime: &mut Runtime, bytes: &[u8]) -> io::Result<()> {
        match runtime.status {
            RelayStatus::Handshaking if runtime.tunnel_active => {
                write_shared(&self.server_in, bytes)?;
            }
            RelayStatus::Handshaking => {
                if append_handshake_data(&mut runtime.client_buffer, bytes) {
                    self.drive_handshake(runtime)?;
                } else {
                    self.fail_handshake(runtime, "Relay client handshake buffer exceeded limit")?;
                    write_shared(&self.server_in, bytes)?;
                }
            }
            RelayStatus::Standby | RelayStatus::Transferring => {
                write_shared(&self.server_in, bytes)?;
                if runtime.status == RelayStatus::Transferring
                    && (bytes == [0x03] || is_transfer_end(&mut runtime.client_end_scan, bytes))
                {
                    self.end_transfer(runtime);
                }
            }
        }
        Ok(())
    }

    fn process_server_data(
        &self,
        runtime: &mut Runtime,
        bytes: &[u8],
        sender: &SyncSender<RelayEvent>,
    ) -> io::Result<()> {
        match runtime.status {
            RelayStatus::Handshaking if runtime.tunnel_active => {
                write_shared(&self.client_out, bytes)?;
            }
            RelayStatus::Handshaking => {
                if append_handshake_data(&mut runtime.server_buffer, bytes) {
                    self.drive_handshake(runtime)?;
                } else {
                    self.fail_handshake(runtime, "Relay server handshake buffer exceeded limit")?;
                    write_shared(&self.client_out, bytes)?;
                }
            }
            RelayStatus::Transferring => {
                write_shared(&self.client_out, bytes)?;
                if is_transfer_end(&mut runtime.server_end_scan, bytes) {
                    self.end_transfer(runtime);
                }
            }
            RelayStatus::Standby => {
                let (output, trigger) = detect_relay_trigger(&mut runtime.trigger_detector, bytes);
                if let Some(mut trigger) = trigger {
                    let tunnel_port = self.start_tunnel_listener(&trigger, sender.clone());
                    trigger.relay_port = tunnel_port.unwrap_or(0);
                    let rewritten = rewrite_relay_trigger(&output, &trigger);
                    runtime.trigger = Some(trigger);
                    runtime.status = RelayStatus::Handshaking;
                    self.write_to_client(runtime, &rewritten)?;
                } else if !output.is_empty() {
                    self.write_to_client(runtime, &output)?;
                }
            }
        }
        Ok(())
    }

    fn finish_trigger_detection(&self, runtime: &mut Runtime) -> Vec<u8> {
        std::mem::take(&mut runtime.trigger_detector)
    }

    fn process_tunnel_client_data(&self, runtime: &mut Runtime, bytes: &[u8]) -> io::Result<()> {
        if runtime.status == RelayStatus::Handshaking {
            if append_handshake_data(&mut runtime.client_buffer, bytes) {
                self.drive_handshake(runtime)?;
            } else {
                self.fail_handshake(
                    runtime,
                    "Relay tunnel client handshake buffer exceeded limit",
                )?;
            }
        } else if runtime.status == RelayStatus::Transferring {
            if let Some(tunnel) = runtime.tunnel.as_mut() {
                tunnel.server.write_all(bytes)?;
                if bytes == [0x03] || is_transfer_end(&mut runtime.client_end_scan, bytes) {
                    self.end_transfer(runtime);
                }
            }
        }
        Ok(())
    }

    fn process_tunnel_server_data(&self, runtime: &mut Runtime, bytes: &[u8]) -> io::Result<()> {
        if runtime.status == RelayStatus::Handshaking {
            if append_handshake_data(&mut runtime.server_buffer, bytes) {
                self.drive_handshake(runtime)?;
            } else {
                self.fail_handshake(
                    runtime,
                    "Relay tunnel server handshake buffer exceeded limit",
                )?;
            }
        } else if runtime.status == RelayStatus::Transferring {
            if let Some(tunnel) = runtime.tunnel.as_mut() {
                tunnel.client.write_all(bytes)?;
                if is_transfer_end(&mut runtime.server_end_scan, bytes) {
                    self.end_transfer(runtime);
                }
            }
        }
        Ok(())
    }

    fn drive_handshake(&self, runtime: &mut Runtime) -> io::Result<()> {
        if runtime.action.is_none() {
            let action_text = match decode_line(&mut runtime.client_buffer, "ACT") {
                Ok(Some(text)) => text,
                Ok(None) => return Ok(()),
                Err(error) => {
                    self.fail_handshake(runtime, &error.to_string())?;
                    return Ok(());
                }
            };
            let mut action: TransferAction = match serde_json::from_str(&action_text) {
                Ok(action) => action,
                Err(error) => {
                    self.fail_handshake(runtime, &format!("Relay decode action error: {error}"))?;
                    return Ok(());
                }
            };
            if !matches!(action.newline.as_str(), "\n" | "!\n" | "\r\n") || action.protocol < 0 {
                self.fail_handshake(runtime, "Relay received an invalid action")?;
                return Ok(());
            }
            runtime.client_is_windows = action.newline == "!\n";
            runtime.tunnel_active = action.tunnel;
            if action.tunnel && runtime.tunnel.is_none() {
                self.fail_handshake(runtime, "Relay tunnel was requested but is not connected")?;
                return Ok(());
            }
            if !runtime.tunnel_active {
                action.support_binary = false;
            }
            action.protocol = action.protocol.min(K_PROTOCOL_VERSION);
            let action_json = match serde_json::to_string(&action) {
                Ok(json) => json,
                Err(error) => {
                    self.fail_handshake(runtime, &format!("Relay encode action error: {error}"))?;
                    return Ok(());
                }
            };
            self.send_protocol_to_server(runtime, "ACT", &action_json)?;
            if !action.confirm {
                self.flush_handshake(runtime, false)?;
                return Ok(());
            }
            runtime.action = Some(action);
        }

        if runtime.action.is_some() {
            let config_text = match decode_line(&mut runtime.server_buffer, "CFG") {
                Ok(Some(text)) => text,
                Ok(None) => return Ok(()),
                Err(error) => {
                    self.fail_handshake(runtime, &error.to_string())?;
                    return Ok(());
                }
            };
            let mut config: TransferConfig = match serde_json::from_str(&config_text) {
                Ok(config) => config,
                Err(error) => {
                    self.fail_handshake(runtime, &format!("Relay decode config error: {error}"))?;
                    return Ok(());
                }
            };
            if !matches!(config.newline.as_str(), "\n" | "!\n" | "\r\n") || config.bufsize <= 0 {
                self.fail_handshake(runtime, "Relay received an invalid config")?;
                return Ok(());
            }
            if let Some(trigger) = runtime.trigger.as_ref() {
                if trigger.parsed.win_server && !runtime.tunnel_active {
                    config.newline = "!\n".to_string();
                }
            }
            if config.tmux_pane_width <= 0 && self.options.terminal_columns > 0 {
                config.tmux_pane_width = self.options.terminal_columns;
            }
            let config_json = match serde_json::to_string(&config) {
                Ok(json) => json,
                Err(error) => {
                    self.fail_handshake(runtime, &format!("Relay encode config error: {error}"))?;
                    return Ok(());
                }
            };
            self.send_protocol_to_client(runtime, "CFG", &config_json)?;
            runtime.status = RelayStatus::Transferring;
            runtime.action = None;
            runtime.client_end_scan.clear();
            runtime.server_end_scan.clear();
            self.notify_transfer_state(true);
            self.flush_handshake(runtime, true)?;
        }
        Ok(())
    }

    fn send_protocol_to_server(
        &self,
        runtime: &mut Runtime,
        kind: &str,
        value: &str,
    ) -> io::Result<()> {
        let newline = if runtime
            .trigger
            .as_ref()
            .is_some_and(|trigger| trigger.parsed.win_server)
            && (!runtime.tunnel_active || kind == "ACT")
        {
            "!\n"
        } else {
            "\n"
        };
        let data = protocol_line(kind, value, newline);
        self.write_to_server(runtime, &data)
    }

    fn send_protocol_to_client(
        &self,
        runtime: &mut Runtime,
        kind: &str,
        value: &str,
    ) -> io::Result<()> {
        let windows = runtime.client_is_windows
            || runtime
                .trigger
                .as_ref()
                .is_some_and(|trigger| trigger.parsed.win_server);
        let newline = if windows && !runtime.tunnel_active {
            "!\n"
        } else {
            "\n"
        };
        let data = protocol_line(kind, value, newline);
        self.write_to_client(runtime, &data)
    }

    fn fail_handshake(&self, runtime: &mut Runtime, message: &str) -> io::Result<()> {
        let client_result = self.send_protocol_to_client(runtime, "FAIL", message);
        let server_result = self.send_protocol_to_server(runtime, "FAIL", message);
        let flush_result = self.flush_handshake(runtime, false);
        client_result.and(server_result).and(flush_result)
    }

    fn flush_handshake(&self, runtime: &mut Runtime, confirmed: bool) -> io::Result<()> {
        if !runtime.client_buffer.is_empty() {
            let buffered = std::mem::take(&mut runtime.client_buffer);
            self.write_to_server(runtime, &buffered)?;
        }
        if !runtime.server_buffer.is_empty() {
            let buffered = std::mem::take(&mut runtime.server_buffer);
            self.write_to_client(runtime, &buffered)?;
        }
        if confirmed {
            runtime.status = RelayStatus::Transferring;
        } else {
            self.end_transfer(runtime);
        }
        Ok(())
    }

    fn write_to_server(&self, runtime: &mut Runtime, bytes: &[u8]) -> io::Result<()> {
        if runtime.tunnel_active {
            if let Some(tunnel) = runtime.tunnel.as_mut() {
                return tunnel.server.write_all(bytes);
            }
        }
        write_shared(&self.server_in, bytes)
    }

    fn write_to_client(&self, runtime: &mut Runtime, bytes: &[u8]) -> io::Result<()> {
        if runtime.tunnel_active {
            if let Some(tunnel) = runtime.tunnel.as_mut() {
                return tunnel.client.write_all(bytes);
            }
        }
        write_shared(&self.client_out, bytes)
    }

    fn install_tunnel(
        &self,
        runtime: &mut Runtime,
        client: TcpStream,
        server: TcpStream,
        sender: SyncSender<RelayEvent>,
    ) -> io::Result<()> {
        let _ = client.set_write_timeout(Some(TUNNEL_TIMEOUT));
        let _ = server.set_write_timeout(Some(TUNNEL_TIMEOUT));
        let client_reader = client.try_clone()?;
        let server_reader = server.try_clone()?;
        runtime.tunnel = Some(TunnelStreams { client, server });
        spawn_tcp_reader(client_reader, sender.clone(), self.closed.clone(), true)?;
        spawn_tcp_reader(server_reader, sender, self.closed.clone(), false)?;
        Ok(())
    }

    fn start_tunnel_listener(
        &self,
        trigger: &RelayTrigger,
        sender: SyncSender<RelayEvent>,
    ) -> Option<i32> {
        if trigger.server_port <= 0 || trigger.server_port > u16::MAX as i32 {
            return None;
        }
        let connector = lock(&self.tunnel_connector).clone()?;
        let listener = TcpListener::bind("127.0.0.1:0").ok()?;
        listener.set_nonblocking(true).ok()?;
        let port = listener.local_addr().ok()?.port() as i32;
        let listener = Arc::new(listener);
        let generation = self.listener_generation.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(old) = lock(&self.tunnel_listener).replace(listener.clone()) {
            drop(old);
        }

        let closed = self.closed.clone();
        let current_generation = self.listener_generation.clone();
        let trigger_id = trigger.unique_id.clone();
        let server_port = trigger.server_port;
        let spawn = thread::Builder::new()
            .name("trzsz-relay-tunnel-listener".to_string())
            .spawn(move || {
                while !closed.load(Ordering::SeqCst)
                    && current_generation.load(Ordering::SeqCst) == generation
                {
                    match listener.accept() {
                        Ok((mut client, _)) => {
                            let _ = client.set_read_timeout(Some(TUNNEL_TIMEOUT));
                            let _ = client.set_write_timeout(Some(TUNNEL_TIMEOUT));
                            let result = accept_tunnel_connection(
                                &mut client,
                                &connector,
                                &trigger_id,
                                server_port,
                                port,
                            );
                            match result {
                                Ok(server) => {
                                    let _ = send_event(
                                        &sender,
                                        RelayEvent::TunnelReady { client, server },
                                        &closed,
                                    );
                                    break;
                                }
                                Err(_) => continue,
                            }
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(RELAY_POLL_INTERVAL);
                        }
                        Err(_) => break,
                    }
                }
            });
        if spawn.is_err() {
            self.stop_tunnel_listener();
            return None;
        }
        Some(port)
    }

    fn stop_tunnel_listener(&self) {
        self.listener_generation.fetch_add(1, Ordering::SeqCst);
        lock(&self.tunnel_listener).take();
    }

    fn end_transfer(&self, runtime: &mut Runtime) {
        if runtime.status == RelayStatus::Transferring {
            self.notify_transfer_state(false);
        }
        runtime.status = RelayStatus::Standby;
        runtime.action = None;
        runtime.tunnel_active = false;
        runtime.client_buffer.clear();
        runtime.server_buffer.clear();
        runtime.client_end_scan.clear();
        runtime.server_end_scan.clear();
        if let Some(tunnel) = runtime.tunnel.take() {
            let _ = tunnel.client.shutdown(Shutdown::Both);
            let _ = tunnel.server.shutdown(Shutdown::Both);
        }
        self.stop_tunnel_listener();
        runtime.trigger = None;
    }

    fn notify_transfer_state(&self, transferring: bool) {
        if let Some(callback) = lock(&self.state_callback).clone() {
            callback(transferring);
        }
    }

    fn finish_runtime(&self, runtime: &mut Runtime) {
        self.end_transfer(runtime);
        self.close();
        close_writer(&self.client_out);
        close_writer(&self.server_in);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn close_writer(writer: &SharedWriter) {
    lock(writer).take();
}

fn write_shared(writer: &SharedWriter, bytes: &[u8]) -> io::Result<()> {
    let mut guard = lock(writer);
    let endpoint = guard
        .as_mut()
        .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "relay endpoint is closed"))?;
    endpoint.write_all(bytes)
}

fn spawn_reader<R: Read + Send + 'static>(
    mut reader: R,
    sender: SyncSender<RelayEvent>,
    closed: Arc<AtomicBool>,
    client: bool,
) -> io::Result<()> {
    thread::Builder::new()
        .name(if client {
            "trzsz-relay-client-reader".to_string()
        } else {
            "trzsz-relay-server-reader".to_string()
        })
        .spawn(move || {
            let mut buffer = vec![0; READ_BUFFER_SIZE];
            loop {
                if closed.load(Ordering::SeqCst) {
                    break;
                }
                match reader.read(&mut buffer) {
                    Ok(0) => {
                        let event = if client {
                            RelayEvent::ClientEof
                        } else {
                            RelayEvent::ServerEof
                        };
                        let _ = send_event(&sender, event, &closed);
                        break;
                    }
                    Ok(size) => {
                        let event = if client {
                            RelayEvent::ClientData(buffer[..size].to_vec())
                        } else {
                            RelayEvent::ServerData(buffer[..size].to_vec())
                        };
                        if !send_event(&sender, event, &closed) {
                            break;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        let event = if client {
                            RelayEvent::ClientError(error)
                        } else {
                            RelayEvent::ServerError(error)
                        };
                        let _ = send_event(&sender, event, &closed);
                        break;
                    }
                }
            }
        })?;
    Ok(())
}

fn spawn_tcp_reader(
    mut reader: TcpStream,
    sender: SyncSender<RelayEvent>,
    closed: Arc<AtomicBool>,
    client: bool,
) -> io::Result<()> {
    let _ = reader.set_read_timeout(Some(RELAY_POLL_INTERVAL));
    thread::Builder::new()
        .name(if client {
            "trzsz-relay-tunnel-client-reader".to_string()
        } else {
            "trzsz-relay-tunnel-server-reader".to_string()
        })
        .spawn(move || {
            let mut buffer = vec![0; READ_BUFFER_SIZE];
            loop {
                if closed.load(Ordering::SeqCst) {
                    break;
                }
                match reader.read(&mut buffer) {
                    Ok(0) => {
                        let event = if client {
                            RelayEvent::TunnelClientEof
                        } else {
                            RelayEvent::TunnelServerEof
                        };
                        let _ = send_event(&sender, event, &closed);
                        break;
                    }
                    Ok(size) => {
                        let event = if client {
                            RelayEvent::TunnelClientData(buffer[..size].to_vec())
                        } else {
                            RelayEvent::TunnelServerData(buffer[..size].to_vec())
                        };
                        if !send_event(&sender, event, &closed) {
                            break;
                        }
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::Interrupted
                                | io::ErrorKind::WouldBlock
                                | io::ErrorKind::TimedOut
                        ) =>
                    {
                        continue;
                    }
                    Err(error) => {
                        let event = if client {
                            RelayEvent::TunnelClientError(error)
                        } else {
                            RelayEvent::TunnelServerError(error)
                        };
                        let _ = send_event(&sender, event, &closed);
                        break;
                    }
                }
            }
        })?;
    Ok(())
}

fn send_event(sender: &SyncSender<RelayEvent>, mut event: RelayEvent, closed: &AtomicBool) -> bool {
    loop {
        if closed.load(Ordering::SeqCst) {
            return false;
        }
        match sender.try_send(event) {
            Ok(()) => return true,
            Err(TrySendError::Disconnected(_)) => return false,
            Err(TrySendError::Full(returned)) => {
                event = returned;
                thread::sleep(Duration::from_millis(5));
            }
        }
    }
}

fn append_handshake_data(buffer: &mut Vec<u8>, bytes: &[u8]) -> bool {
    let Some(size) = buffer.len().checked_add(bytes.len()) else {
        return false;
    };
    if size > MAX_HANDSHAKE_BUFFER {
        return false;
    }
    buffer.extend_from_slice(bytes);
    true
}

fn decode_line(buffer: &mut Vec<u8>, expected_type: &str) -> io::Result<Option<String>> {
    let Some(newline) = buffer.iter().position(|&byte| byte == b'\n') else {
        if buffer.len() > MAX_PROTOCOL_LINE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "relay protocol line exceeds maximum size",
            ));
        }
        return Ok(None);
    };
    if newline > MAX_PROTOCOL_LINE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "relay protocol line exceeds maximum size",
        ));
    }
    let mut line: Vec<u8> = buffer.drain(..=newline).collect();
    line.pop();
    if matches!(line.last(), Some(b'\r' | b'!')) {
        line.pop();
    }
    let marker = format!("#{expected_type}:");
    let start = line
        .windows(marker.len())
        .rposition(|window| window == marker.as_bytes())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("relay expected #{expected_type} protocol line"),
            )
        })?;
    let encoded = std::str::from_utf8(&line[start + marker.len()..]).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid relay line: {error}"),
        )
    })?;
    let decoded = escape::decode_string(encoded)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    String::from_utf8(decoded)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

fn protocol_line(kind: &str, value: &str, newline: &str) -> Vec<u8> {
    format!("#{kind}:{}{newline}", escape::encode_string(value)).into_bytes()
}

fn parse_trigger(bytes: &[u8]) -> Option<(usize, usize, RelayTrigger)> {
    let marker = bytes
        .windows(TRIGGER_MARKER.len())
        .rposition(|window| window == TRIGGER_MARKER)?;
    let end = bytes[marker..]
        .iter()
        .position(|&byte| byte == b'\r' || byte == b'\n')?
        + marker;
    if end.saturating_sub(marker) > MAX_TRIGGER_LINE {
        return None;
    }
    let text = std::str::from_utf8(&bytes[marker + TRIGGER_MARKER.len()..end]).ok()?;
    let mut parts = text.split(':');
    let mode_text = parts.next()?;
    let mode = match mode_text {
        "S" => 'S',
        "R" => 'R',
        "D" => 'D',
        _ => return None,
    };
    let version_text = parts.next()?;
    let version = TrzszVersion::parse(version_text)?;
    let unique_id = parts.next().unwrap_or_default();
    if !unique_id.is_empty() && !unique_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let port_text = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || (!port_text.is_empty() && !port_text.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }
    let server_port = if port_text.is_empty() {
        0
    } else {
        port_text.parse::<i32>().ok()?
    };
    let win_server = unique_id == "1" || (unique_id.len() == 13 && unique_id.ends_with("10"));
    let parsed = TrzszTrigger {
        mode,
        version: Some(version),
        unique_id: unique_id.to_string(),
        win_server,
        tunnel_port: server_port,
        tmux_prefix: String::new(),
        tmux_pane_id: String::new(),
    };
    Some((
        marker,
        end,
        RelayTrigger {
            parsed,
            unique_id: unique_id.to_string(),
            server_port,
            relay_port: 0,
        },
    ))
}

fn detect_relay_trigger(pending: &mut Vec<u8>, bytes: &[u8]) -> (Vec<u8>, Option<RelayTrigger>) {
    pending.extend_from_slice(bytes);
    let mut output = Vec::new();
    loop {
        let Some(marker) = pending
            .windows(TRIGGER_MARKER.len())
            .rposition(|window| window == TRIGGER_MARKER)
        else {
            let keep = longest_marker_suffix(pending);
            let emit = pending.len().saturating_sub(keep);
            output.extend(pending.drain(..emit));
            return (output, None);
        };
        let Some(line_end_rel) = pending[marker..]
            .iter()
            .position(|&byte| byte == b'\r' || byte == b'\n')
        else {
            if pending.len().saturating_sub(marker) > MAX_TRIGGER_LINE {
                output.push(pending.remove(0));
                continue;
            }
            output.extend(pending.drain(..marker));
            return (output, None);
        };
        let line_end = marker + line_end_rel;
        let mut consumed_end = line_end;
        if pending.get(line_end) == Some(&b'\r') && pending.get(line_end + 1) == Some(&b'\n') {
            consumed_end += 2;
        } else {
            consumed_end += 1;
        }
        let Some((trigger_marker, match_end, trigger)) = parse_trigger(&pending[..consumed_end])
        else {
            output.extend(pending.drain(..consumed_end));
            continue;
        };
        output.extend(pending.drain(..trigger_marker));
        let line_length = consumed_end - trigger_marker;
        let mut line: Vec<u8> = pending.drain(..line_length).collect();
        let relative_match_end = match_end.saturating_sub(trigger_marker);
        if relative_match_end <= line.len() {
            line.splice(
                relative_match_end..relative_match_end,
                b"#R".iter().copied(),
            );
        }
        output.extend_from_slice(&line);
        output.extend(pending.drain(..));
        return (output, Some(trigger));
    }
}

fn longest_marker_suffix(bytes: &[u8]) -> usize {
    (1..TRIGGER_MARKER.len().min(bytes.len() + 1))
        .rev()
        .find(|&count| bytes.ends_with(&TRIGGER_MARKER[..count]))
        .unwrap_or(0)
}

fn rewrite_relay_trigger(output: &[u8], trigger: &RelayTrigger) -> Vec<u8> {
    if trigger.relay_port <= 0 || trigger.server_port <= 0 || trigger.unique_id.is_empty() {
        return output.to_vec();
    }
    let from = format!(":{}:{}", trigger.unique_id, trigger.server_port).into_bytes();
    let to = format!(":{}:{}", trigger.unique_id, trigger.relay_port).into_bytes();
    let mut rewritten = Vec::with_capacity(output.len() + to.len().saturating_sub(from.len()));
    let mut index = 0;
    while index < output.len() {
        if output[index..].starts_with(&from) {
            rewritten.extend_from_slice(&to);
            index += from.len();
        } else {
            rewritten.push(output[index]);
            index += 1;
        }
    }
    rewritten
}

fn accept_tunnel_connection(
    client: &mut TcpStream,
    connector: &Arc<TunnelConnector>,
    unique_id: &str,
    server_port: i32,
    relay_port: i32,
) -> io::Result<TcpStream> {
    let hello_id = if unique_id.len() > 2 {
        &unique_id[..unique_id.len() - 2]
    } else {
        unique_id
    };
    let client_hello_relay = format!("::TRZSZ::CLIENT::HELLO::{hello_id}:{relay_port}");
    let client_hello_server = format!("::TRZSZ::CLIENT::HELLO::{hello_id}:{server_port}");
    let server_hello_server = format!("::TRZSZ::SERVER::HELLO::{hello_id}:{server_port}");
    let server_hello_relay = format!("::TRZSZ::SERVER::HELLO::{hello_id}:{relay_port}");

    let mut received = vec![0; client_hello_relay.len()];
    client.read_exact(&mut received)?;
    if received != client_hello_relay.as_bytes() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid client tunnel hello",
        ));
    }
    let mut server = connector(server_port)?;
    let _ = server.set_read_timeout(Some(TUNNEL_TIMEOUT));
    let _ = server.set_write_timeout(Some(TUNNEL_TIMEOUT));
    server.write_all(client_hello_server.as_bytes())?;
    let mut response = vec![0; server_hello_server.len()];
    server.read_exact(&mut response)?;
    if response != server_hello_server.as_bytes() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid server tunnel hello",
        ));
    }
    client.write_all(server_hello_relay.as_bytes())?;
    Ok(server)
}

fn is_transfer_end(tail: &mut Vec<u8>, bytes: &[u8]) -> bool {
    const MARKERS: [&[u8]; 3] = [b"#EXIT:", b"#FAIL:", b"#fail:"];
    let keep = MARKERS
        .iter()
        .map(|marker| marker.len() - 1)
        .max()
        .unwrap_or(0);
    let mut combined = std::mem::take(tail);
    combined.extend_from_slice(bytes);
    let found = MARKERS.iter().any(|marker| {
        combined
            .windows(marker.len())
            .any(|window| window == *marker)
    });
    let suffix_start = combined.len().saturating_sub(keep);
    tail.extend_from_slice(&combined[suffix_start..]);
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::time::Instant;

    struct ChannelReader(Receiver<Vec<u8>>);

    impl Read for ChannelReader {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            let bytes = match self.0.recv() {
                Ok(bytes) => bytes,
                Err(_) => return Ok(0),
            };
            let size = output.len().min(bytes.len());
            output[..size].copy_from_slice(&bytes[..size]);
            if size < bytes.len() {
                // Test messages fit within the relay read buffer.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "test chunk too large",
                ));
            }
            Ok(size)
        }
    }

    struct ChannelWriter(Sender<Vec<u8>>);

    impl Write for ChannelWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .send(bytes.to_vec())
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "test receiver closed"))?;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn wait_for_data(receiver: &Receiver<Vec<u8>>, needle: &[u8]) -> Vec<u8> {
        let until = Instant::now() + Duration::from_secs(3);
        let mut all = Vec::new();
        while Instant::now() < until {
            match receiver.recv_timeout(Duration::from_millis(50)) {
                Ok(bytes) => {
                    all.extend_from_slice(&bytes);
                    if all.windows(needle.len()).any(|window| window == needle) {
                        return all;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        all
    }

    fn channel_relay(
        client_reader: Box<dyn Read + Send>,
        server_reader: Box<dyn Read + Send>,
        client_sender: Sender<Vec<u8>>,
        client_output: Sender<Vec<u8>>,
        server_output: Sender<Vec<u8>>,
    ) -> TrzszRelay {
        let _ = client_sender;
        TrzszRelay::new(
            client_reader,
            Box::new(ChannelWriter(client_output)),
            Box::new(ChannelWriter(server_output)),
            server_reader,
            TrzszOptions::default(),
        )
    }

    #[test]
    fn trigger_is_rewritten_with_relay_suffix_and_fragments_are_detected() {
        let mut detector = Vec::new();
        let (first, trigger) = detect_relay_trigger(
            &mut detector,
            b"prefix\x1b[s::TRZSZ:TRANSFER:S:1.2.3:1234567890123:",
        );
        assert_eq!(first, b"prefix\x1b[s");
        assert!(trigger.is_none());
        let (second, trigger) = detect_relay_trigger(&mut detector, b"7000\r\n");
        assert_eq!(second, b"::TRZSZ:TRANSFER:S:1.2.3:1234567890123:7000#R\r\n");
        let trigger = trigger.expect("valid relay trigger");
        assert_eq!(trigger.unique_id, "1234567890123");
        assert_eq!(trigger.server_port, 7000);
        assert_eq!(trigger.parsed.mode, 'S');
    }
    #[test]
    fn handshake_framing_negotiates_action_and_config_and_reports_state() {
        let (client_tx, client_rx) = mpsc::channel();
        let (client_output_tx, client_output_rx) = mpsc::channel();
        let (server_input_tx, server_input_rx) = mpsc::channel();
        let (server_output_tx, server_output_rx) = mpsc::channel();
        let action = TransferAction {
            protocol: K_PROTOCOL_VERSION + 3,
            support_binary: true,
            ..TransferAction::default()
        };
        let action_line = protocol_line("ACT", &serde_json::to_string(&action).unwrap(), "\n");
        let config_line = protocol_line(
            "CFG",
            &serde_json::to_string(&TransferConfig::default()).unwrap(),
            "\n",
        );
        let relay = TrzszRelay::new(
            Box::new(ChannelReader(client_rx)),
            Box::new(ChannelWriter(client_output_tx)),
            Box::new(ChannelWriter(server_input_tx)),
            Box::new(ChannelReader(server_output_rx)),
            TrzszOptions::default(),
        );
        let (state_tx, state_rx) = mpsc::channel();
        relay.set_transfer_state_callback(move |state| {
            let _ = state_tx.send(state);
        });
        let thread = thread::spawn(move || relay.run());

        server_output_tx
            .send(b"::TRZSZ:TRANSFER:S:1.2.3:1234567890123:7000\r\n".to_vec())
            .unwrap();
        let trigger = wait_for_data(&client_output_rx, b"#R\r\n");
        assert!(trigger.windows(b"#R\r\n".len()).any(|w| w == b"#R\r\n"));
        client_tx.send(action_line).unwrap();
        let server_bytes = wait_for_data(&server_input_rx, b"#ACT:");
        let act_line = server_bytes
            .split(|byte| *byte == b'\n')
            .find(|line| line.starts_with(b"#ACT:"))
            .expect("relayed ACT line");
        let act_payload = std::str::from_utf8(&act_line[b"#ACT:".len()..]).unwrap();
        let decoded = escape::decode_string(act_payload).unwrap();
        let forwarded: TransferAction = serde_json::from_slice(&decoded).unwrap();
        assert_eq!(forwarded.protocol, K_PROTOCOL_VERSION);
        assert!(!forwarded.support_binary);

        server_output_tx.send(config_line).unwrap();
        let client_bytes = wait_for_data(&client_output_rx, b"#CFG:");
        assert!(client_bytes.windows(b"#CFG:".len()).any(|w| w == b"#CFG:"));
        assert_eq!(state_rx.recv_timeout(Duration::from_secs(2)).unwrap(), true);
        client_tx.send(b"#EXIT:finished\n".to_vec()).unwrap();
        drop(client_tx);
        drop(server_output_tx);

        let result = thread.join().unwrap();
        assert!(result.is_ok());
        assert_eq!(
            state_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            false
        );
    }

    #[test]
    fn ordinary_streams_relay_bytes_in_both_directions() {
        let (client_tx, client_rx) = mpsc::channel();
        let (client_output_tx, client_output_rx) = mpsc::channel();
        let (server_input_tx, server_input_rx) = mpsc::channel();
        let (server_output_tx, server_output_rx) = mpsc::channel();
        let relay = channel_relay(
            Box::new(ChannelReader(client_rx)),
            Box::new(ChannelReader(server_output_rx)),
            client_tx.clone(),
            client_output_tx,
            server_input_tx,
        );
        client_tx.send(b"client bytes\0\xff".to_vec()).unwrap();
        server_output_tx
            .send(b"server bytes\0\xfe".to_vec())
            .unwrap();
        drop(client_tx);
        drop(server_output_tx);
        relay.run().unwrap();
        assert_eq!(server_input_rx.recv().unwrap(), b"client bytes\0\xff");
        assert_eq!(client_output_rx.recv().unwrap(), b"server bytes\0\xfe");
    }

    #[test]
    fn malformed_handshake_returns_fail_without_panicking() {
        let (client_tx, client_rx) = mpsc::channel();
        let (client_output_tx, client_output_rx) = mpsc::channel();
        let (server_input_tx, server_input_rx) = mpsc::channel();
        let (server_output_tx, server_output_rx) = mpsc::channel();
        let relay = TrzszRelay::new(
            Box::new(ChannelReader(client_rx)),
            Box::new(ChannelWriter(client_output_tx)),
            Box::new(ChannelWriter(server_input_tx)),
            Box::new(ChannelReader(server_output_rx)),
            TrzszOptions::default(),
        );
        let thread = thread::spawn(move || relay.run());
        server_output_tx
            .send(b"::TRZSZ:TRANSFER:R:1.2.3:1234567890123\n".to_vec())
            .unwrap();
        let _ = wait_for_data(&client_output_rx, b"#R\n");
        client_tx.send(b"#ACT:not-base64\n".to_vec()).unwrap();
        let client_response = wait_for_data(&client_output_rx, b"#FAIL:");
        assert!(
            client_response
                .windows(b"#FAIL:".len())
                .any(|w| w == b"#FAIL:")
        );
        let server_response = wait_for_data(&server_input_rx, b"#FAIL:");
        assert!(
            server_response
                .windows(b"#FAIL:".len())
                .any(|w| w == b"#FAIL:")
        );
        drop(client_tx);
        drop(server_output_tx);
        assert!(thread.join().unwrap().is_ok());
    }

    #[test]
    fn tunnel_handshake_rewrites_local_port_and_relays_protocol_over_loopback() {
        let upstream = TcpListener::bind("127.0.0.1:0").unwrap();
        let upstream_port = upstream.local_addr().unwrap().port() as i32;
        let (upstream_done_tx, upstream_done_rx) = mpsc::channel();
        let upstream_thread = thread::spawn(move || {
            let (mut stream, _) = upstream.accept().unwrap();
            let hello = format!("::TRZSZ::CLIENT::HELLO::12345678901:{upstream_port}");
            let mut request = vec![0; hello.len()];
            stream.read_exact(&mut request).unwrap();
            assert_eq!(request, hello.as_bytes());
            stream
                .write_all(
                    format!("::TRZSZ::SERVER::HELLO::12345678901:{upstream_port}").as_bytes(),
                )
                .unwrap();
            let action = read_test_line(&mut stream);
            let _ = upstream_done_tx.send(action);
            let config = protocol_line(
                "CFG",
                &serde_json::to_string(&TransferConfig::default()).unwrap(),
                "\n",
            );
            stream.write_all(&config).unwrap();
            let mut payload = [0; 4];
            stream.read_exact(&mut payload).unwrap();
            assert_eq!(&payload, b"ping");
        });

        let (client_tx, client_rx) = mpsc::channel();
        let (client_output_tx, client_output_rx) = mpsc::channel();
        let (server_input_tx, _server_input_rx) = mpsc::channel();
        let (server_output_tx, server_output_rx) = mpsc::channel();
        let relay = Arc::new(TrzszRelay::new(
            Box::new(ChannelReader(client_rx)),
            Box::new(ChannelWriter(client_output_tx)),
            Box::new(ChannelWriter(server_input_tx)),
            Box::new(ChannelReader(server_output_rx)),
            TrzszOptions::default(),
        ));
        relay.set_tunnel_connector(move |port| {
            TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], port as u16)))
        });
        let relay_run = relay.clone();
        let run_thread = thread::spawn(move || relay_run.run());
        server_output_tx
            .send(
                format!("::TRZSZ:TRANSFER:S:1.2.3:1234567890123:{upstream_port}\r\n").into_bytes(),
            )
            .unwrap();
        let output = wait_for_data(&client_output_rx, b"#R\r\n");
        let line = output
            .split(|byte| *byte == b'\n')
            .find(|line| line.windows(2).any(|window| window == b"#R"))
            .expect("rewritten trigger");
        let advertised_port = parse_advertised_port(line).expect("relay tunnel port");
        let mut tunnel_client = TcpStream::connect(("127.0.0.1", advertised_port)).unwrap();
        tunnel_client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let hello_id = "12345678901";
        tunnel_client
            .write_all(format!("::TRZSZ::CLIENT::HELLO::{hello_id}:{advertised_port}").as_bytes())
            .unwrap();
        let response = format!("::TRZSZ::SERVER::HELLO::{hello_id}:{advertised_port}");
        let mut response_bytes = vec![0; response.len()];
        tunnel_client.read_exact(&mut response_bytes).unwrap();
        assert_eq!(response_bytes, response.as_bytes());

        let action = TransferAction {
            tunnel: true,
            ..TransferAction::default()
        };
        tunnel_client
            .write_all(&protocol_line(
                "ACT",
                &serde_json::to_string(&action).unwrap(),
                "\n",
            ))
            .unwrap();
        let forwarded_action = upstream_done_rx
            .recv_timeout(Duration::from_secs(3))
            .unwrap();
        assert!(forwarded_action.starts_with(b"#ACT:"));
        assert!(read_test_line(&mut tunnel_client).starts_with(b"#CFG:"));
        tunnel_client.write_all(b"ping").unwrap();
        upstream_thread.join().unwrap();
        relay.close();
        drop(client_tx);
        drop(server_output_tx);
        assert!(run_thread.join().unwrap().is_ok());
    }

    fn read_test_line(stream: &mut TcpStream) -> Vec<u8> {
        let mut line = Vec::new();
        let mut byte = [0; 1];
        while line.len() < MAX_PROTOCOL_LINE {
            if stream.read_exact(&mut byte).is_err() {
                break;
            }
            line.push(byte[0]);
            if byte[0] == b'\n' {
                break;
            }
        }
        line
    }

    fn parse_advertised_port(line: &[u8]) -> Option<u16> {
        let trigger = std::str::from_utf8(line).ok()?;
        let part = trigger.split("::TRZSZ:TRANSFER:").nth(1)?;
        let port = part
            .trim_end_matches(['\r', '\n'])
            .split(':')
            .nth(3)?
            .split('#')
            .next()?;
        port.parse().ok()
    }
    struct BlockingReader(Receiver<()>);

    impl Read for BlockingReader {
        fn read(&mut self, _output: &mut [u8]) -> io::Result<usize> {
            let _ = self.0.recv();
            Ok(0)
        }
    }

    #[test]
    fn close_wakes_blocking_readers_and_ends_run() {
        let (client_wake_tx, client_wake_rx) = mpsc::channel();
        let (server_wake_tx, server_wake_rx) = mpsc::channel();
        let (client_input_tx, client_input_rx) = mpsc::channel();
        let (server_output_tx, server_output_rx) = mpsc::channel();
        let relay = Arc::new(TrzszRelay::new(
            Box::new(BlockingReader(client_input_rx)),
            Box::new(std::io::sink()),
            Box::new(std::io::sink()),
            Box::new(BlockingReader(server_output_rx)),
            TrzszOptions::default(),
        ));
        relay.set_shutdown_handlers(
            Some(Arc::new(move || {
                let _ = client_wake_tx.send(());
            })),
            Some(Arc::new(move || {
                let _ = server_wake_tx.send(());
            })),
        );
        let running = relay.clone();
        let thread = thread::spawn(move || running.run());
        thread::sleep(Duration::from_millis(20));
        relay.close();

        client_wake_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        server_wake_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(thread.join().unwrap().is_ok());
        drop(client_input_tx);
        drop(server_output_tx);
    }
    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("injected relay writer failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn io_error_still_wakes_blocked_readers() {
        let (client_wake_tx, client_wake_rx) = mpsc::channel();
        let (server_wake_tx, server_wake_rx) = mpsc::channel();
        let (client_input_tx, client_input_rx) = mpsc::channel();
        let (server_output_tx, server_output_rx) = mpsc::channel();
        let relay = Arc::new(TrzszRelay::new(
            Box::new(BlockingReader(client_input_rx)),
            Box::new(FailingWriter),
            Box::new(std::io::sink()),
            Box::new(ChannelReader(server_output_rx)),
            TrzszOptions::default(),
        ));
        relay.set_shutdown_handlers(
            Some(Arc::new(move || {
                let _ = client_wake_tx.send(());
            })),
            Some(Arc::new(move || {
                let _ = server_wake_tx.send(());
            })),
        );
        let running = relay.clone();
        let run_thread = thread::spawn(move || running.run());
        server_output_tx.send(b"server output".to_vec()).unwrap();

        let error = run_thread.join().unwrap().unwrap_err();
        assert_eq!(error.to_string(), "injected relay writer failure");
        client_wake_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        server_wake_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(client_input_tx);
        drop(server_output_tx);
    }
}
