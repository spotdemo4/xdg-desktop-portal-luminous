use calloop::{
    RegistrationToken,
    channel::{Sender, channel},
};
use enumflags2::BitFlags;
use reis::{
    calloop::{EisListenerSource, EisRequestSource, EisRequestSourceEvent},
    eis::{self, device::DeviceType as EisDeviceType},
    request::{Connection, DeviceCapability, EisRequest},
};

use crate::session::DeviceType as PortalDeviceType;
use std::{
    collections::HashMap,
    io,
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};

#[derive(Default)]
struct ContextState {
    capabilities: BitFlags<DeviceCapability>,
    seat: Option<reis::request::Seat>,
    device_keyboard: Option<reis::request::Device>,
    device_pointer: Option<reis::request::Device>,
    pointer_capabilities: BitFlags<DeviceCapability>,
    device_touch: Option<reis::request::Device>,
    sequence: u32,
}

impl ContextState {
    fn handle_request(
        &mut self,
        connection: &Connection,
        request: &EisRequest,
    ) -> calloop::PostAction {
        match request {
            EisRequest::Disconnect => {
                return calloop::PostAction::Remove;
            }
            EisRequest::Bind(request) => {
                let capabilities = request.capabilities & self.capabilities;

                let keyboard_enabled = capabilities.contains(DeviceCapability::Keyboard);
                if !keyboard_enabled {
                    if let Some(device) = self.device_keyboard.take() {
                        device.remove();
                    }
                } else if self.device_keyboard.is_none() {
                    self.device_keyboard = Some(add_device(
                        "keyboard",
                        BitFlags::from_flag(DeviceCapability::Keyboard),
                        |_| {},
                        &request.seat,
                        connection,
                        &mut self.sequence,
                    ));
                }

                let pointer_capabilities = capabilities
                    & (DeviceCapability::Pointer
                        | DeviceCapability::PointerAbsolute
                        | DeviceCapability::Button
                        | DeviceCapability::Scroll);
                if pointer_capabilities != self.pointer_capabilities {
                    if let Some(device) = self.device_pointer.take() {
                        device.remove();
                    }
                    self.pointer_capabilities = pointer_capabilities;
                    if !pointer_capabilities.is_empty() {
                        self.device_pointer = Some(add_device(
                            "pointer",
                            pointer_capabilities,
                            |_| {},
                            &request.seat,
                            connection,
                            &mut self.sequence,
                        ));
                    }
                }

                let touch_enabled = capabilities.contains(DeviceCapability::Touch);
                if !touch_enabled {
                    if let Some(device) = self.device_touch.take() {
                        device.remove();
                    }
                } else if self.device_touch.is_none() {
                    self.device_touch = Some(add_device(
                        "touch",
                        BitFlags::from_flag(DeviceCapability::Touch),
                        |_| {},
                        &request.seat,
                        connection,
                        &mut self.sequence,
                    ));
                }
            }
            _ => {}
        }

        calloop::PostAction::Continue
    }
}

fn add_device(
    name: &str,
    capabilities: BitFlags<DeviceCapability>,
    before_done_cb: impl for<'a> FnOnce(&'a reis::request::Device),
    seat: &reis::request::Seat,
    connection: &Connection,
    sequence: &mut u32,
) -> reis::request::Device {
    let device = seat.add_device(
        Some(name),
        EisDeviceType::Virtual,
        capabilities,
        before_done_cb,
    );
    device.resumed();
    if connection.context_type() == eis::handshake::ContextType::Receiver {
        *sequence += 1;
        device.start_emulating(*sequence);
    }
    device
}

fn device_capabilities(devices: BitFlags<PortalDeviceType>) -> BitFlags<DeviceCapability> {
    let mut capabilities = BitFlags::empty();
    if devices.contains(PortalDeviceType::Keyboard) {
        capabilities |= DeviceCapability::Keyboard;
    }
    if devices.contains(PortalDeviceType::Pointer) {
        capabilities |= DeviceCapability::Pointer
            | DeviceCapability::PointerAbsolute
            | DeviceCapability::Scroll
            | DeviceCapability::Button;
    }
    if devices.contains(PortalDeviceType::TouchScreen) {
        capabilities |= DeviceCapability::Touch;
    }
    capabilities
}

struct State {
    handle: calloop::LoopHandle<'static, Self>,
    sender: mpsc::Sender<InputEvent>,
    clients: HashMap<String, RegistrationToken>,
}

impl State {
    fn handle_new_connection(
        &mut self,
        context: eis::Context,
        session_handle: String,
        devices: BitFlags<PortalDeviceType>,
    ) -> io::Result<calloop::PostAction> {
        tracing::info!(
            "New connection for session {}: {:?}",
            session_handle,
            context
        );

        let source = EisRequestSource::new(context, 1);
        let mut context_state = ContextState {
            capabilities: device_capabilities(devices),
            ..Default::default()
        };
        let session_handle_clone = session_handle.clone();
        self.handle
            .insert_source(source, move |event, connected_state, state| {
                Ok(match event {
                    Ok(event) => Self::handle_request_source_event(
                        &mut context_state,
                        connected_state,
                        event,
                        &state.sender,
                        &session_handle_clone,
                    ),
                    Err(err) => {
                        tracing::error!("Error communicating with client: {err}");
                        calloop::PostAction::Remove
                    }
                })
            })
            .unwrap();

        Ok(calloop::PostAction::Continue)
    }

    fn handle_request_source_event(
        context_state: &mut ContextState,
        connection: &Connection,
        event: EisRequestSourceEvent,
        sender: &mpsc::Sender<InputEvent>,
        session_handle: &str,
    ) -> calloop::PostAction {
        match event {
            EisRequestSourceEvent::Connected => {
                let seat = connection.add_seat(Some("default"), context_state.capabilities);

                context_state.seat = Some(seat);
            }
            EisRequestSourceEvent::Request(request) => {
                match &request {
                    EisRequest::PointerMotion(e) => {
                        let _ = sender.send(InputEvent::PointerMotion {
                            session_handle: session_handle.to_string(),
                            dx: e.dx as f64,
                            dy: e.dy as f64,
                        });
                    }
                    EisRequest::PointerMotionAbsolute(e) => {
                        let _ = sender.send(InputEvent::PointerMotionAbsolute {
                            session_handle: session_handle.to_string(),
                            x: e.dx_absolute as f64,
                            y: e.dy_absolute as f64,
                        });
                    }
                    EisRequest::Button(e) => {
                        let _ = sender.send(InputEvent::PointerButton {
                            session_handle: session_handle.to_string(),
                            button: e.button as i32,
                            state: e.state as u32,
                        });
                    }
                    EisRequest::ScrollDelta(e) => {
                        let _ = sender.send(InputEvent::PointerAxis {
                            session_handle: session_handle.to_string(),
                            dx: e.dx as f64,
                            dy: e.dy as f64,
                        });
                    }
                    EisRequest::ScrollDiscrete(e) => {
                        if e.discrete_dx != 0 {
                            let _ = sender.send(InputEvent::PointerAxisDiscrete {
                                session_handle: session_handle.to_string(),
                                axis: 1, // Horizontal
                                steps: e.discrete_dx,
                            });
                        }
                        if e.discrete_dy != 0 {
                            let _ = sender.send(InputEvent::PointerAxisDiscrete {
                                session_handle: session_handle.to_string(),
                                axis: 0, // Vertical
                                steps: e.discrete_dy,
                            });
                        }
                    }
                    EisRequest::KeyboardKey(e) => {
                        let _ = sender.send(InputEvent::KeyboardKeycode {
                            session_handle: session_handle.to_string(),
                            keycode: e.key as i32,
                            state: e.state as u32,
                        });
                    }
                    EisRequest::TouchDown(e) => {
                        let _ = sender.send(InputEvent::TouchDown {
                            session_handle: session_handle.to_string(),
                            slot: e.touch_id,
                            x: e.x as f64,
                            y: e.y as f64,
                        });
                    }
                    EisRequest::TouchMotion(e) => {
                        let _ = sender.send(InputEvent::TouchMotion {
                            session_handle: session_handle.to_string(),
                            slot: e.touch_id,
                            x: e.x as f64,
                            y: e.y as f64,
                        });
                    }
                    EisRequest::TouchUp(e) => {
                        let _ = sender.send(InputEvent::TouchUp {
                            session_handle: session_handle.to_string(),
                            slot: e.touch_id,
                        });
                    }
                    _ => {}
                }

                let res = context_state.handle_request(connection, &request);
                if res != calloop::PostAction::Continue {
                    return res;
                }
            }
        }

        let _ = connection.flush();

        calloop::PostAction::Continue
    }
}

#[allow(clippy::enum_variant_names)]
pub enum EisServerMsg {
    NewListener(eis::Listener, String, BitFlags<PortalDeviceType>),
    StopListener(String),
    ActiveListener(String),
    RemoveListener(String),
}

pub enum InputEvent {
    PointerMotion {
        session_handle: String,
        dx: f64,
        dy: f64,
    },
    PointerMotionAbsolute {
        session_handle: String,
        x: f64,
        y: f64,
    },
    PointerButton {
        session_handle: String,
        button: i32,
        state: u32,
    },
    PointerAxis {
        session_handle: String,
        dx: f64,
        dy: f64,
    },
    PointerAxisDiscrete {
        session_handle: String,
        axis: u32,
        steps: i32,
    },
    KeyboardKeycode {
        session_handle: String,
        keycode: i32,
        state: u32,
    },
    TouchMotion {
        session_handle: String,
        slot: u32,
        x: f64,
        y: f64,
    },
    TouchDown {
        session_handle: String,
        slot: u32,
        x: f64,
        y: f64,
    },
    TouchUp {
        session_handle: String,
        slot: u32,
    },
}

pub fn start() -> (Sender<EisServerMsg>, Receiver<InputEvent>) {
    let (tx, msg_channel) = channel();
    let (input_tx, input_rx) = mpsc::channel();

    thread::spawn(move || {
        let mut event_loop = calloop::EventLoop::<State>::try_new().unwrap();
        let handle = event_loop.handle();
        let mut state = State {
            handle: handle.clone(),
            sender: input_tx,
            clients: HashMap::new(),
        };

        let _ = handle.insert_source(msg_channel, |event, _, state| {
            if let calloop::channel::Event::Msg(msg) = event {
                match msg {
                    EisServerMsg::NewListener(listener, session_handle, devices) => {
                        let listener_source = EisListenerSource::new(listener);
                        let session_handle_2 = session_handle.clone();
                        let token = state
                            .handle
                            .insert_source(
                                listener_source,
                                move |context, (), state: &mut State| {
                                    state.handle_new_connection(
                                        context,
                                        session_handle.clone(),
                                        devices,
                                    )
                                },
                            )
                            .unwrap();
                        state.clients.insert(session_handle_2, token);
                    }
                    EisServerMsg::StopListener(session) => {
                        let Some(token) = state.clients.get(&session) else {
                            return;
                        };
                        let _ = state.handle.disable(token);
                    }
                    EisServerMsg::ActiveListener(session) => {
                        let Some(token) = state.clients.get(&session) else {
                            return;
                        };
                        let _ = state.handle.enable(token);
                    }
                    EisServerMsg::RemoveListener(session) => {
                        let Some(token) = state.clients.remove(&session) else {
                            return;
                        };
                        state.handle.remove(token);
                    }
                }
            }
        });

        loop {
            event_loop
                .dispatch(Duration::from_millis(100), &mut state)
                .unwrap();
        }
    });

    (tx, input_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eis_capabilities_match_authorized_devices() {
        let keyboard = device_capabilities(PortalDeviceType::Keyboard.into());
        assert!(keyboard.contains(DeviceCapability::Keyboard));
        assert!(!keyboard.contains(DeviceCapability::Pointer));
        assert!(!keyboard.contains(DeviceCapability::Touch));

        let pointer = device_capabilities(PortalDeviceType::Pointer.into());
        assert!(pointer.contains(DeviceCapability::Pointer));
        assert!(pointer.contains(DeviceCapability::PointerAbsolute));
        assert!(pointer.contains(DeviceCapability::Button));
        assert!(pointer.contains(DeviceCapability::Scroll));
        assert!(!pointer.contains(DeviceCapability::Keyboard));

        let touch = device_capabilities(PortalDeviceType::TouchScreen.into());
        assert!(touch.contains(DeviceCapability::Touch));
        assert!(!touch.contains(DeviceCapability::Pointer));
    }
}
