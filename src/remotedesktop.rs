mod dispatch;
mod eis_server;
mod remote_thread;
mod state;

use libwayshot::WayshotConnection;
use libwaysip::{SelectionType, WaySip};
pub use remote_thread::RemoteControl;
use stream_message::SERVER_SOCK;
use wayland_client::protocol::wl_output;

use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, LazyLock, Mutex as StdMutex};

use calloop::channel::Sender;
use enumflags2::BitFlags;
use reis::eis;
use rustix::fd::AsFd;
use rustix::io;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use zbus::zvariant::{
    Fd, ObjectPath, OwnedObjectPath, OwnedValue, Type, Value,
    as_value::{self, optional},
};
use zbus::{interface, object_server::ResponseDispatchNotifier};

use crate::PortalResponse;
use crate::input_capture::BarrierInfo;
use crate::pipewirethread::CastTarget;
use crate::pipewirethread::ScreencastThread;
use crate::request::RequestInterface;
use crate::session::{
    DeviceType, PersistMode, SESSIONS, Session, SessionType, SourceType, append_session,
};
use crate::utils::get_selection_from_socket;

pub use self::eis_server::{EisServerMsg, InputEvent};
pub use self::remote_thread::InputRequest;
use std::hash::Hash;

use std::sync::atomic::{self, AtomicU32};

type EisServerSender = Sender<EisServerMsg>;
type InputEventReceiver = Arc<StdMutex<Receiver<InputEvent>>>;
pub(crate) type RestoreData = (String, u32, OwnedValue);

const RESTORE_DATA_VENDOR: &str = "Luminous";
const RESTORE_DATA_VERSION: u32 = 1;
const MAX_OUTPUT_NAME_LEN: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Type, Value, zbus::zvariant::OwnedValue)]
struct RestorePayloadV1 {
    output_name: String,
    devices: u32,
    screen_share_enabled: bool,
    clipboard_enabled: bool,
    source_types: u32,
    multiple: bool,
    persist_mode: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RemoteAuthorization {
    output_name: String,
    devices: BitFlags<DeviceType>,
    screen_share_enabled: bool,
    clipboard_enabled: bool,
    source_types: BitFlags<SourceType>,
    multiple: bool,
    persist_mode: PersistMode,
}

impl RemoteAuthorization {
    pub(crate) fn new(
        output_name: String,
        devices: BitFlags<DeviceType>,
        screen_share_enabled: bool,
        clipboard_enabled: bool,
        source_types: BitFlags<SourceType>,
        multiple: bool,
        persist_mode: PersistMode,
    ) -> Self {
        Self {
            output_name,
            devices,
            screen_share_enabled,
            clipboard_enabled,
            source_types,
            multiple,
            persist_mode,
        }
    }

    fn matches_request(
        &self,
        devices: BitFlags<DeviceType>,
        screen_share_enabled: bool,
        clipboard_enabled: bool,
        source_types: BitFlags<SourceType>,
        multiple: bool,
        persist_mode: PersistMode,
    ) -> bool {
        self.devices == devices
            && self.screen_share_enabled == screen_share_enabled
            && self.clipboard_enabled == clipboard_enabled
            && self.source_types == source_types
            && self.multiple == multiple
            && persist_mode as u32 <= self.persist_mode as u32
    }

    fn has_restorable_target(&self) -> bool {
        !self.output_name.is_empty()
            && self.output_name.len() <= MAX_OUTPUT_NAME_LEN
            && (!self.screen_share_enabled
                || (self.source_types.contains(SourceType::Monitor) && !self.multiple))
    }
}

fn parse_persist_mode(value: u32) -> Option<PersistMode> {
    match value {
        0 => Some(PersistMode::DoNot),
        1 => Some(PersistMode::Application),
        2 => Some(PersistMode::ExplicitlyRevoked),
        _ => None,
    }
}

pub(crate) fn parse_restore_data(restore_data: RestoreData) -> Option<RemoteAuthorization> {
    let (vendor, version, payload) = restore_data;
    if vendor != RESTORE_DATA_VENDOR || version != RESTORE_DATA_VERSION {
        return None;
    }

    let payload = RestorePayloadV1::try_from(payload).ok()?;
    let authorization = RemoteAuthorization::new(
        payload.output_name,
        BitFlags::from_bits(payload.devices).ok()?,
        payload.screen_share_enabled,
        payload.clipboard_enabled,
        BitFlags::from_bits(payload.source_types).ok()?,
        payload.multiple,
        parse_persist_mode(payload.persist_mode)?,
    );
    authorization
        .has_restorable_target()
        .then_some(authorization)
}

fn build_restore_data(authorization: &RemoteAuthorization) -> zbus::fdo::Result<RestoreData> {
    let payload = RestorePayloadV1 {
        output_name: authorization.output_name.clone(),
        devices: authorization.devices.bits(),
        screen_share_enabled: authorization.screen_share_enabled,
        clipboard_enabled: authorization.clipboard_enabled,
        source_types: authorization.source_types.bits(),
        multiple: authorization.multiple,
        persist_mode: authorization.persist_mode as u32,
    };
    let payload = OwnedValue::try_from(payload)
        .map_err(|error| zbus::Error::Failure(format!("cannot serialize restore data: {error}")))?;

    Ok((
        RESTORE_DATA_VENDOR.to_string(),
        RESTORE_DATA_VERSION,
        payload,
    ))
}

pub static EIS_SERVER: LazyLock<(EisServerSender, InputEventReceiver)> = LazyLock::new(|| {
    let (tx, rx) = eis_server::start();
    (tx, Arc::new(StdMutex::new(rx)))
});

pub fn get_input_receiver() -> InputEventReceiver {
    EIS_SERVER.1.clone()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
/// The id of the window.
///
/// Internally Iced reserves `window::Id::MAIN` for the first window spawned.
pub struct ZoneId(u32);

static COUNT: AtomicU32 = AtomicU32::new(0);

impl ZoneId {
    /// Creates a new unique window [`Id`].
    pub fn unique() -> ZoneId {
        ZoneId(COUNT.fetch_add(1, atomic::Ordering::Relaxed))
    }
    pub fn value(&self) -> u32 {
        self.0
    }
}

#[derive(Type, Debug, Default, Serialize, Deserialize)]
/// Specified options for a [`Screencast::create_session`] request.
#[zvariant(signature = "dict")]
struct SessionCreateResult {
    #[serde(with = "as_value")]
    handle_token: String,
}

#[derive(Clone, Serialize, Deserialize, Type, Default, Debug)]
/// A PipeWire stream.
pub struct Stream(u32, StreamProperties);

#[derive(Clone, Default, Type, Debug, Serialize, Deserialize)]
/// The stream properties.
#[zvariant(signature = "dict")]
struct StreamProperties {
    #[serde(with = "optional", skip_serializing_if = "Option::is_none", default)]
    id: Option<String>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none", default)]
    position: Option<(i32, i32)>,
    #[serde(with = "as_value")]
    size: (i32, i32),
    #[serde(with = "as_value")]
    source_type: SourceType,
}

// TODO: this is copy from ashpd, but the dict is a little different from xdg_desktop_portal
#[derive(Clone, Default, Debug, Type, Serialize, Deserialize)]
#[zvariant(signature = "dict")]
struct RemoteStartReturnValue {
    #[serde(with = "as_value")]
    streams: Vec<Stream>,
    #[serde(with = "as_value")]
    devices: BitFlags<DeviceType>,
    #[serde(with = "as_value")]
    clipboard_enabled: bool,
    #[serde(with = "as_value")]
    screen_share_enabled: bool,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none", default)]
    restore_data: Option<RestoreData>,
}

fn remote_start_response(
    session_handle: &ObjectPath<'_>,
    value: RemoteStartReturnValue,
) -> ResponseDispatchNotifier<PortalResponse<RemoteStartReturnValue>> {
    let clipboard_enabled = value.clipboard_enabled;
    let (response, listener) = ResponseDispatchNotifier::new(PortalResponse::Success(value));
    if clipboard_enabled {
        let key: OwnedObjectPath = session_handle.to_owned().into();
        tokio::spawn(async move {
            listener.await; // resolves once the Start reply is on the bus
            crate::clipboard::spawn_clipboard_forwarder(&key).await;
        });
    }
    response
}

fn remote_start_other() -> ResponseDispatchNotifier<PortalResponse<RemoteStartReturnValue>> {
    ResponseDispatchNotifier::new(PortalResponse::Other).0
}

fn build_remote_start_value(
    streams: Vec<Stream>,
    authorization: &RemoteAuthorization,
    persist_mode: PersistMode,
    clipboard_enabled: bool,
) -> zbus::fdo::Result<RemoteStartReturnValue> {
    let restore_data = if persist_mode == PersistMode::DoNot {
        None
    } else {
        Some(build_restore_data(authorization)?)
    };

    Ok(RemoteStartReturnValue {
        streams,
        devices: authorization.devices,
        clipboard_enabled,
        screen_share_enabled: authorization.screen_share_enabled,
        restore_data,
    })
}

#[derive(Type, Debug, Default, Deserialize, Serialize)]
/// Specified options for a [`RemoteDesktop::select_devices`] request.
#[zvariant(signature = "dict")]
pub struct SelectDevicesOptions {
    /// A string that will be used as the last element of the handle.
    /// The device types to request remote controlling of. Default is all.
    #[serde(with = "optional", skip_serializing_if = "Option::is_none", default)]
    pub types: Option<BitFlags<DeviceType>>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none", default)]
    pub restore_data: Option<RestoreData>,
    #[serde(with = "optional", skip_serializing_if = "Option::is_none", default)]
    pub persist_mode: Option<PersistMode>,
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, Type)]
pub struct CursorPosition {
    x: f64,
    y: f64,
}

pub struct RemoteSessionData {
    pub session_handle: String,
    pub cast_thread: Option<ScreencastThread>,
    pub remote_control: RemoteControl,
    pub zones: Vec<Zone>,
    pub zone_id: ZoneId,
    pub barriers: Vec<BarrierInfo>,
    authorization: RemoteAuthorization,
    persist_mode: PersistMode,
    cursor: CursorPosition,
    activation_id: u32,
}

impl RemoteSessionData {
    pub fn new(
        session_handle: String,
        cast_thread: Option<ScreencastThread>,
        remote_control: RemoteControl,
        zones: Vec<Zone>,
        authorization: RemoteAuthorization,
        persist_mode: PersistMode,
    ) -> Self {
        Self {
            session_handle,
            cast_thread,
            remote_control,
            zones,
            zone_id: ZoneId::unique(),
            authorization,
            persist_mode,
            cursor: CursorPosition::default(),
            barriers: Vec::new(),
            activation_id: 0,
        }
    }
    pub fn step(&mut self) {
        self.activation_id += 1;
    }
    pub fn activation_id(&self) -> u32 {
        self.activation_id
    }
    pub fn cursor_position(&self) -> CursorPosition {
        self.cursor
    }
    pub(crate) fn devices(&self) -> BitFlags<DeviceType> {
        self.authorization.devices
    }
    pub fn update_cursor(&mut self, event: InputRequest) {
        match event {
            InputRequest::PointerMotionAbsolute { x, y } => {
                self.cursor = CursorPosition { x, y };
            }
            InputRequest::PointerMotion { dx, dy } => {
                self.cursor.x += dx;
                self.cursor.y += dy;
            }
            _ => {}
        }
    }
}

#[derive(Debug, Type, Serialize, Deserialize, Clone, Copy)]
pub struct Zone {
    pub width: u32,
    pub height: u32,
    pub x_offset: i32,
    pub y_offset: i32,
}

impl RemoteSessionData {
    fn stop(&self) {
        self.remote_control.stop();
        if let Some(cast_thread) = &self.cast_thread {
            cast_thread.stop();
        }
        EIS_SERVER
            .0
            .send(EisServerMsg::RemoveListener(self.session_handle.clone()))
            .unwrap();
    }

    fn streams(&self) -> Vec<Stream> {
        let Some(cast_thread) = &self.cast_thread else {
            return vec![];
        };
        vec![Stream(cast_thread.node_id(), StreamProperties::default())]
    }
}

pub static REMOTE_SESSIONS: LazyLock<Arc<Mutex<Vec<RemoteSessionData>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(Vec::new())));

pub async fn append_remote_session(session: RemoteSessionData) {
    let mut sessions = REMOTE_SESSIONS.lock().await;
    sessions.push(session)
}

pub async fn remove_remote_session(path: &str) {
    let mut sessions = REMOTE_SESSIONS.lock().await;
    let Some(index) = sessions
        .iter()
        .position(|the_session| the_session.session_handle == path)
    else {
        return;
    };
    sessions[index].stop();
    tracing::info!("session {} is stopped", sessions[index].session_handle);
    sessions.remove(index);
}

pub async fn enable_eis_listener(session_handle: ObjectPath<'_>) {
    EIS_SERVER
        .0
        .send(EisServerMsg::ActiveListener(session_handle.to_string()))
        .unwrap();
}
pub async fn disable_eis_listener(session_handle: ObjectPath<'_>) {
    EIS_SERVER
        .0
        .send(EisServerMsg::StopListener(session_handle.to_string()))
        .unwrap();
}

fn input_request_device(event: &InputRequest) -> Option<DeviceType> {
    match event {
        InputRequest::PointerMotion { .. }
        | InputRequest::PointerMotionAbsolute { .. }
        | InputRequest::PointerButton { .. }
        | InputRequest::PointerAxis { .. }
        | InputRequest::PointerAxisDiscrete { .. } => Some(DeviceType::Pointer),
        InputRequest::KeyboardKeycode { .. } | InputRequest::KeyboardKeysym { .. } => {
            Some(DeviceType::Keyboard)
        }
        InputRequest::TouchMotion { .. }
        | InputRequest::TouchDown { .. }
        | InputRequest::TouchUp { .. } => Some(DeviceType::TouchScreen),
        InputRequest::Exit => None,
    }
}

fn input_request_is_authorized(devices: BitFlags<DeviceType>, event: &InputRequest) -> bool {
    input_request_device(event).is_none_or(|device| devices.contains(device))
}

async fn notify_input_event(
    session_handle: ObjectPath<'_>,
    event: InputRequest,
) -> zbus::fdo::Result<()> {
    let mut remote_sessions = REMOTE_SESSIONS.lock().await;
    let Some(session) = remote_sessions
        .iter_mut()
        .find(|session| session.session_handle == session_handle.to_string())
    else {
        return Ok(());
    };
    if !input_request_is_authorized(session.authorization.devices, &event) {
        return Err(zbus::Error::Failure("input device is not authorized".to_string()).into());
    }
    session.update_cursor(event);
    let remote_control = &session.remote_control;
    remote_control
        .sender
        .send(event)
        .map_err(|_| zbus::Error::Failure("Send failed".to_string()))?;
    Ok(())
}

fn option_bool(options: &HashMap<String, Value<'_>>, key: &str) -> bool {
    options
        .get(key)
        .and_then(|value| value.downcast_ref::<bool>().ok())
        .unwrap_or(false)
}

pub async fn handle_input_event(event: InputEvent) {
    let (session_handle, request) = match event {
        InputEvent::PointerMotion {
            session_handle,
            dx,
            dy,
        } => (session_handle, InputRequest::PointerMotion { dx, dy }),
        InputEvent::PointerMotionAbsolute {
            session_handle,
            x,
            y,
        } => (session_handle, InputRequest::PointerMotionAbsolute { x, y }),
        InputEvent::PointerButton {
            session_handle,
            button,
            state,
        } => (
            session_handle,
            InputRequest::PointerButton { button, state },
        ),
        InputEvent::PointerAxis {
            session_handle,
            dx,
            dy,
        } => (
            session_handle,
            InputRequest::PointerAxis {
                dx,
                dy,
                finish: false,
            },
        ),
        InputEvent::PointerAxisDiscrete {
            session_handle,
            axis,
            steps,
        } => (
            session_handle,
            InputRequest::PointerAxisDiscrete { axis, steps },
        ),
        InputEvent::KeyboardKeycode {
            session_handle,
            keycode,
            state,
        } => (
            session_handle,
            InputRequest::KeyboardKeycode { keycode, state },
        ),
        InputEvent::TouchDown {
            session_handle,
            slot,
            x,
            y,
        } => (session_handle, InputRequest::TouchDown { slot, x, y }),
        InputEvent::TouchMotion {
            session_handle,
            slot,
            x,
            y,
        } => (session_handle, InputRequest::TouchMotion { slot, x, y }),
        InputEvent::TouchUp {
            session_handle,
            slot,
        } => (session_handle, InputRequest::TouchUp { slot }),
    };

    if let Ok(path) = ObjectPath::try_from(session_handle) {
        let _ = notify_input_event(path, request).await;
    }
}

pub struct RemoteDesktopBackend;

#[interface(name = "org.freedesktop.impl.portal.RemoteDesktop")]
impl RemoteDesktopBackend {
    #[zbus(property, name = "version")]
    fn version(&self) -> u32 {
        2
    }

    #[zbus(property)]
    fn available_device_types(&self) -> u32 {
        (DeviceType::Keyboard | DeviceType::Pointer | DeviceType::TouchScreen).bits()
    }

    async fn create_session(
        &self,
        request_handle: ObjectPath<'_>,
        session_handle: ObjectPath<'_>,
        app_id: String,
        _options: HashMap<String, Value<'_>>,
        #[zbus(object_server)] server: &zbus::ObjectServer,
    ) -> zbus::fdo::Result<PortalResponse<SessionCreateResult>> {
        tracing::info!(
            "Start remoting: path :{}, appid: {}",
            request_handle.as_str(),
            app_id
        );
        server
            .at(
                request_handle.clone(),
                RequestInterface {
                    handle_path: request_handle.clone().into(),
                    close_action: None,
                },
            )
            .await?;
        let current_session = Session::new(session_handle.clone(), SessionType::Remote);
        append_session(current_session.clone()).await;
        server.at(session_handle.clone(), current_session).await?;
        Ok(PortalResponse::Success(SessionCreateResult {
            handle_token: session_handle.to_string(),
        }))
    }

    async fn select_devices(
        &self,
        _request_handle: ObjectPath<'_>,
        session_handle: ObjectPath<'_>,
        _app_id: String,
        options: SelectDevicesOptions,
    ) -> zbus::fdo::Result<PortalResponse<HashMap<String, OwnedValue>>> {
        let remote_restore = options.restore_data.and_then(parse_restore_data);
        let types = options.types;
        let persist_mode = options.persist_mode;
        let mut locked_sessions = SESSIONS.lock().await;
        let Some(index) = locked_sessions
            .iter()
            .position(|this_session| this_session.handle_path == session_handle.clone().into())
        else {
            tracing::warn!("No session is created or it is removed");
            return Ok(PortalResponse::Other);
        };
        if locked_sessions[index].session_type != SessionType::Remote {
            return Ok(PortalResponse::Other);
        }
        locked_sessions[index].set_remote_options(types, persist_mode, remote_restore);
        Ok(PortalResponse::Success(HashMap::new()))
    }

    async fn start(
        &self,
        _request_handle: ObjectPath<'_>,
        session_handle: ObjectPath<'_>,
        _app_id: String,
        _parent_window: String,
        _options: HashMap<String, Value<'_>>,
        #[zbus(connection)] dbus_connection: &zbus::Connection,
    ) -> zbus::fdo::Result<ResponseDispatchNotifier<PortalResponse<RemoteStartReturnValue>>> {
        let locked_sessions = SESSIONS.lock().await;
        let Some(index) = locked_sessions
            .iter()
            .position(|this_session| this_session.handle_path == session_handle.clone().into())
        else {
            tracing::warn!("No session is created or it is removed");
            return Ok(remote_start_other());
        };

        let current_session = locked_sessions[index].clone();
        if current_session.session_type != SessionType::Remote {
            return Ok(remote_start_other());
        }
        drop(locked_sessions);

        let remote_sessions = REMOTE_SESSIONS.lock().await;
        if let Some(session) = remote_sessions
            .iter()
            .find(|session| session.session_handle == session_handle.to_string())
        {
            let streams = session.streams();
            let authorization = session.authorization.clone();
            let persist_mode = session.persist_mode;
            drop(remote_sessions);
            let clipboard_enabled = authorization.clipboard_enabled
                && crate::clipboard::ensure_clipboard_session(
                    &session_handle,
                    dbus_connection.clone(),
                )
                .await;
            let value =
                build_remote_start_value(streams, &authorization, persist_mode, clipboard_enabled)?;
            return Ok(remote_start_response(&session_handle, value));
        }
        drop(remote_sessions);

        let restored_output_name = current_session
            .remote_restore
            .as_ref()
            .filter(|authorization| {
                authorization.matches_request(
                    current_session.device_type,
                    current_session.screen_share_enabled,
                    current_session.clipboard_requested,
                    current_session.source_type,
                    current_session.multiple,
                    current_session.persist_mode,
                )
            })
            .map(|authorization| authorization.output_name.as_str());

        let mut streams = vec![];
        let mut cast_thread = None;
        let connection = libwayshot::WayshotConnection::new().unwrap();
        let RemoteInfo {
            output_name,
            width,
            height,
            x,
            y,
            wl_output,
        } = get_monitor_info(&connection, restored_output_name)?;
        let persist_mode = if output_name.is_empty()
            || output_name.len() > MAX_OUTPUT_NAME_LEN
            || (current_session.screen_share_enabled
                && (!current_session.source_type.contains(SourceType::Monitor)
                    || current_session.multiple))
        {
            PersistMode::DoNot
        } else {
            current_session.persist_mode
        };
        let authorization = RemoteAuthorization::new(
            output_name,
            current_session.device_type,
            current_session.screen_share_enabled,
            current_session.clipboard_requested,
            current_session.source_type,
            current_session.multiple,
            persist_mode,
        );

        if authorization.screen_share_enabled {
            let show_cursor = current_session.cursor_mode.show_cursor();
            let cast_thread_target = ScreencastThread::start_cast(
                show_cursor,
                CastTarget::Screen(wl_output),
                connection,
            )
            .await
            .map_err(|e| {
                zbus::Error::Failure(format!("cannot start pipewire stream, error: {e}"))
            })?;

            let node_id = cast_thread_target.node_id();
            streams.push(Stream(
                node_id,
                StreamProperties {
                    size: (width, height),
                    source_type: SourceType::Monitor,
                    ..Default::default()
                },
            ));
            cast_thread = Some(cast_thread_target);
        }
        let remote_control = RemoteControl::init(x as u32, y as u32, width as u32, height as u32);
        let clipboard_enabled = authorization.clipboard_enabled
            && crate::clipboard::ensure_clipboard_session(&session_handle, dbus_connection.clone())
                .await;

        append_remote_session(RemoteSessionData::new(
            session_handle.to_string(),
            cast_thread,
            remote_control,
            vec![Zone {
                x_offset: x,
                y_offset: y,
                width: width as u32,
                height: height as u32,
            }],
            authorization.clone(),
            persist_mode,
        ))
        .await;
        let value =
            build_remote_start_value(streams, &authorization, persist_mode, clipboard_enabled)?;
        Ok(remote_start_response(&session_handle, value))
    }

    // keyboard and else
    async fn notify_pointer_motion(
        &self,
        session_handle: ObjectPath<'_>,
        _options: HashMap<String, Value<'_>>,
        dx: f64,
        dy: f64,
    ) -> zbus::fdo::Result<()> {
        notify_input_event(session_handle, InputRequest::PointerMotion { dx, dy }).await
    }

    async fn notify_pointer_motion_absolute(
        &self,
        session_handle: ObjectPath<'_>,
        _options: HashMap<String, Value<'_>>,
        _steam: u32,
        x: f64,
        y: f64,
    ) -> zbus::fdo::Result<()> {
        notify_input_event(session_handle, InputRequest::PointerMotionAbsolute { x, y }).await
    }

    async fn notify_pointer_button(
        &self,
        session_handle: ObjectPath<'_>,
        _options: HashMap<String, Value<'_>>,
        button: i32,
        state: u32,
    ) -> zbus::fdo::Result<()> {
        notify_input_event(
            session_handle,
            InputRequest::PointerButton { button, state },
        )
        .await
    }

    async fn notify_pointer_axis(
        &self,
        session_handle: ObjectPath<'_>,
        options: HashMap<String, Value<'_>>,
        dx: f64,
        dy: f64,
    ) -> zbus::fdo::Result<()> {
        notify_input_event(
            session_handle,
            InputRequest::PointerAxis {
                dx,
                dy,
                finish: option_bool(&options, "finish"),
            },
        )
        .await
    }

    async fn notify_pointer_axis_discrete(
        &self,
        session_handle: ObjectPath<'_>,
        _options: HashMap<String, Value<'_>>,
        axis: u32,
        steps: i32,
    ) -> zbus::fdo::Result<()> {
        notify_input_event(
            session_handle,
            InputRequest::PointerAxisDiscrete { axis, steps },
        )
        .await
    }

    async fn notify_keyboard_keycode(
        &self,
        session_handle: ObjectPath<'_>,
        _options: HashMap<String, Value<'_>>,
        keycode: i32,
        state: u32,
    ) -> zbus::fdo::Result<()> {
        notify_input_event(
            session_handle,
            InputRequest::KeyboardKeycode { keycode, state },
        )
        .await
    }

    async fn notify_keyboard_keysym(
        &self,
        session_handle: ObjectPath<'_>,
        _options: HashMap<String, Value<'_>>,
        keysym: i32,
        state: u32,
    ) -> zbus::fdo::Result<()> {
        notify_input_event(
            session_handle,
            InputRequest::KeyboardKeysym { keysym, state },
        )
        .await
    }

    async fn notify_touch_down(
        &self,
        session_handle: ObjectPath<'_>,
        _options: HashMap<String, Value<'_>>,
        _stream: u32,
        slot: u32,
        x: f64,
        y: f64,
    ) -> zbus::fdo::Result<()> {
        notify_input_event(session_handle, InputRequest::TouchDown { slot, x, y }).await
    }

    async fn notify_touch_motion(
        &self,
        session_handle: ObjectPath<'_>,
        _options: HashMap<String, Value<'_>>,
        _stream: u32,
        slot: u32,
        x: f64,
        y: f64,
    ) -> zbus::fdo::Result<()> {
        notify_input_event(session_handle, InputRequest::TouchMotion { slot, x, y }).await
    }

    async fn notify_touch_up(
        &self,
        session_handle: ObjectPath<'_>,
        _options: HashMap<String, Value<'_>>,
        slot: u32,
    ) -> zbus::fdo::Result<()> {
        notify_input_event(session_handle, InputRequest::TouchUp { slot }).await
    }

    #[zbus(name = "ConnectToEIS")]
    async fn connect_to_eis(
        &self,
        session_handle: ObjectPath<'_>,
        _app_id: String,
        _options: HashMap<String, Value<'_>>,
    ) -> zbus::fdo::Result<Fd<'_>> {
        let session_key = session_handle.to_string();
        let remote_sessions = REMOTE_SESSIONS.lock().await;
        let devices = remote_sessions
            .iter()
            .find(|session| session.session_handle == session_key)
            .map(|session| session.authorization.devices)
            .ok_or_else(|| zbus::Error::Failure("remote session is not started".to_string()))?;
        drop(remote_sessions);

        let listener = eis::Listener::bind_auto()
            .map_err(|e| zbus::Error::Failure(format!("Failed to create EIS listener: {}", e)))?;

        let fd = io::dup(listener.as_fd()).map_err(|e| zbus::Error::Failure(e.to_string()))?;
        EIS_SERVER
            .0
            .send(EisServerMsg::NewListener(listener, session_key, devices))
            .unwrap();

        Ok(Fd::from(fd))
    }
}

#[derive(Debug, Clone)]
pub struct RemoteInfo {
    pub output_name: String,
    pub x: i32,
    pub y: i32,
    pub width: i32,
    pub height: i32,
    wl_output: wl_output::WlOutput,
}

fn space_size(outputs: &[libwayshot::output::OutputInfo]) -> libwayshot::Size<i32> {
    let mut space_width = 0;
    let mut space_height = 0;

    for output in outputs {
        let libwayshot::region::Position { x, y } = output.logical_region.inner.position;
        let libwayshot::Size { width, height } = output.physical_size;
        space_width = space_width.max(width as i32 + x);
        space_height = space_height.max(height as i32 + y)
    }

    libwayshot::Size {
        width: space_width,
        height: space_height,
    }
}

fn find_unique_output_name<'a>(
    names: impl IntoIterator<Item = &'a str>,
    restored_name: &str,
) -> Option<usize> {
    let mut matches = names
        .into_iter()
        .enumerate()
        .filter(|(_, name)| *name == restored_name);
    let (index, _) = matches.next()?;
    matches.next().is_none().then_some(index)
}

fn checked_output_index(output_count: usize, index: u32) -> zbus::fdo::Result<usize> {
    let index = index as usize;
    (index < output_count).then_some(index).ok_or_else(|| {
        zbus::Error::Failure(format!("output picker returned invalid index {index}")).into()
    })
}

fn remote_info_from_output(
    output: &libwayshot::output::OutputInfo,
    width: i32,
    height: i32,
) -> RemoteInfo {
    let libwayshot::region::Position { x, y } = output.logical_region.inner.position;
    RemoteInfo {
        output_name: output.name.clone(),
        x,
        y,
        width,
        height,
        wl_output: output.wl_output.clone(),
    }
}

pub fn get_monitor_info(
    connection: &WayshotConnection,
    restored_name: Option<&str>,
) -> zbus::fdo::Result<RemoteInfo> {
    let outputs = connection.get_all_outputs();
    let libwayshot::Size { width, height } = space_size(outputs);

    if let Some(restored_name) = restored_name
        && let Some(index) = find_unique_output_name(
            outputs.iter().map(|output| output.name.as_str()),
            restored_name,
        )
    {
        return Ok(remote_info_from_output(&outputs[index], width, height));
    }

    if SERVER_SOCK.exists() {
        let monitors: Vec<String> = outputs.iter().map(|output| output.name.clone()).collect();
        let index = checked_output_index(outputs.len(), get_selection_from_socket(monitors)?)?;
        Ok(remote_info_from_output(&outputs[index], width, height))
    } else {
        let info = match WaySip::new()
            .with_connection(connection.conn.clone())
            .with_selection_type(SelectionType::Screen)
            .get()
        {
            Ok(Some(info)) => info,
            Ok(None) => return Err(zbus::Error::Failure("You cancel it".to_string()).into()),
            Err(e) => return Err(zbus::Error::Failure(format!("wayland error, {e}")).into()),
        };

        let screen_info = info.screen_info;
        let libwaysip::Position { x, y } = screen_info.get_position();
        Ok(RemoteInfo {
            output_name: screen_info.name,
            x,
            y,
            width,
            height,
            wl_output: screen_info.wl_output,
        })
    }
}

pub fn get_monitor_info_from_socket(
    connection: &WayshotConnection,
) -> zbus::fdo::Result<RemoteInfo> {
    get_monitor_info(connection, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authorization_with_persist(persist_mode: PersistMode) -> RemoteAuthorization {
        RemoteAuthorization::new(
            "DP-1".to_string(),
            DeviceType::Keyboard | DeviceType::Pointer,
            true,
            true,
            SourceType::Monitor.into(),
            false,
            persist_mode,
        )
    }

    fn authorization() -> RemoteAuthorization {
        authorization_with_persist(PersistMode::Application)
    }

    fn restore_data_from_payload(payload: RestorePayloadV1) -> RestoreData {
        (
            RESTORE_DATA_VENDOR.to_string(),
            RESTORE_DATA_VERSION,
            OwnedValue::try_from(payload).unwrap(),
        )
    }

    #[test]
    fn restore_data_has_expected_signatures() {
        assert_eq!(<RestoreData as Type>::SIGNATURE, "(suv)");
        assert_eq!(RestorePayloadV1::SIGNATURE, "(subbubu)");
    }

    #[test]
    fn restore_data_round_trips() {
        let authorization = authorization();
        let restore_data = build_restore_data(&authorization).unwrap();
        assert_eq!(parse_restore_data(restore_data), Some(authorization));
    }

    #[test]
    fn invalid_restore_envelopes_are_ignored() {
        let authorization = authorization();
        let (_, version, payload) = build_restore_data(&authorization).unwrap();
        assert!(parse_restore_data(("Other".to_string(), version, payload)).is_none());

        let (_, _, payload) = build_restore_data(&authorization).unwrap();
        assert!(
            parse_restore_data((
                RESTORE_DATA_VENDOR.to_string(),
                RESTORE_DATA_VERSION + 1,
                payload,
            ))
            .is_none()
        );

        assert!(
            parse_restore_data((
                RESTORE_DATA_VENDOR.to_string(),
                RESTORE_DATA_VERSION,
                OwnedValue::from(42_u32),
            ))
            .is_none()
        );
    }

    #[test]
    fn invalid_restore_payloads_are_ignored() {
        let empty_output = RestorePayloadV1 {
            output_name: String::new(),
            devices: DeviceType::Keyboard as u32,
            screen_share_enabled: false,
            clipboard_enabled: false,
            source_types: SourceType::Monitor as u32,
            multiple: false,
            persist_mode: PersistMode::Application as u32,
        };
        assert!(parse_restore_data(restore_data_from_payload(empty_output)).is_none());

        let unknown_device = RestorePayloadV1 {
            output_name: "DP-1".to_string(),
            devices: 1 << 8,
            screen_share_enabled: false,
            clipboard_enabled: false,
            source_types: SourceType::Monitor as u32,
            multiple: false,
            persist_mode: PersistMode::Application as u32,
        };
        assert!(parse_restore_data(restore_data_from_payload(unknown_device)).is_none());

        let window_only = RestorePayloadV1 {
            output_name: "DP-1".to_string(),
            devices: DeviceType::Keyboard as u32,
            screen_share_enabled: true,
            clipboard_enabled: false,
            source_types: SourceType::Window as u32,
            multiple: false,
            persist_mode: PersistMode::Application as u32,
        };
        assert!(parse_restore_data(restore_data_from_payload(window_only)).is_none());

        let multiple = RestorePayloadV1 {
            output_name: "DP-1".to_string(),
            devices: DeviceType::Keyboard as u32,
            screen_share_enabled: true,
            clipboard_enabled: false,
            source_types: SourceType::Monitor as u32,
            multiple: true,
            persist_mode: PersistMode::Application as u32,
        };
        assert!(parse_restore_data(restore_data_from_payload(multiple)).is_none());

        let invalid_persist_mode = RestorePayloadV1 {
            output_name: "DP-1".to_string(),
            devices: DeviceType::Keyboard as u32,
            screen_share_enabled: false,
            clipboard_enabled: false,
            source_types: SourceType::Monitor as u32,
            multiple: false,
            persist_mode: 3,
        };
        assert!(parse_restore_data(restore_data_from_payload(invalid_persist_mode)).is_none());

        let oversized_output = RestorePayloadV1 {
            output_name: "x".repeat(MAX_OUTPUT_NAME_LEN + 1),
            devices: DeviceType::Keyboard as u32,
            screen_share_enabled: false,
            clipboard_enabled: false,
            source_types: SourceType::Monitor as u32,
            multiple: false,
            persist_mode: PersistMode::Application as u32,
        };
        assert!(parse_restore_data(restore_data_from_payload(oversized_output)).is_none());
    }

    #[test]
    fn restored_output_name_must_match_exactly_once() {
        assert_eq!(
            find_unique_output_name(["DP-1", "HDMI-A-1"], "DP-1"),
            Some(0)
        );
        assert_eq!(find_unique_output_name(["DP-1"], "DP-2"), None);
        assert_eq!(find_unique_output_name(["DP-1", "DP-1"], "DP-1"), None);
    }

    #[test]
    fn restored_authorization_requires_an_exact_request_match() {
        let authorization = authorization();
        let devices = DeviceType::Keyboard | DeviceType::Pointer;
        let source_types = BitFlags::from_flag(SourceType::Monitor);
        assert!(authorization.matches_request(
            devices,
            true,
            true,
            source_types,
            false,
            PersistMode::Application,
        ));
        assert!(authorization.matches_request(
            devices,
            true,
            true,
            source_types,
            false,
            PersistMode::DoNot,
        ));
        assert!(!authorization.matches_request(
            DeviceType::Keyboard.into(),
            true,
            true,
            source_types,
            false,
            PersistMode::Application,
        ));
        assert!(!authorization.matches_request(
            devices,
            false,
            true,
            source_types,
            false,
            PersistMode::Application,
        ));
        assert!(!authorization.matches_request(
            devices,
            true,
            false,
            source_types,
            false,
            PersistMode::Application,
        ));
        assert!(!authorization.matches_request(
            devices,
            true,
            true,
            SourceType::Window.into(),
            false,
            PersistMode::Application,
        ));
        assert!(!authorization.matches_request(
            devices,
            true,
            true,
            source_types,
            true,
            PersistMode::Application,
        ));
        assert!(!authorization.matches_request(
            devices,
            true,
            true,
            source_types,
            false,
            PersistMode::ExplicitlyRevoked,
        ));
    }

    #[test]
    fn restore_data_is_returned_for_both_persistent_modes() {
        let transient_authorization = authorization_with_persist(PersistMode::DoNot);
        let transient = build_remote_start_value(
            Vec::new(),
            &transient_authorization,
            PersistMode::DoNot,
            true,
        )
        .unwrap();
        assert!(transient.restore_data.is_none());

        for persist_mode in [PersistMode::Application, PersistMode::ExplicitlyRevoked] {
            let authorization = authorization_with_persist(persist_mode);
            let persistent =
                build_remote_start_value(Vec::new(), &authorization, persist_mode, true).unwrap();
            let restored = parse_restore_data(persistent.restore_data.unwrap()).unwrap();
            assert_eq!(restored, authorization);
        }
    }

    #[test]
    fn input_requests_are_checked_against_authorized_devices() {
        let keyboard = BitFlags::from_flag(DeviceType::Keyboard);
        assert!(input_request_is_authorized(
            keyboard,
            &InputRequest::KeyboardKeycode {
                keycode: 1,
                state: 1,
            },
        ));
        assert!(!input_request_is_authorized(
            keyboard,
            &InputRequest::PointerMotion { dx: 1.0, dy: 1.0 },
        ));
        assert!(!input_request_is_authorized(
            keyboard,
            &InputRequest::TouchUp { slot: 0 },
        ));
        assert!(input_request_is_authorized(keyboard, &InputRequest::Exit));
    }

    #[test]
    fn headless_picker_index_is_bounds_checked() {
        assert_eq!(checked_output_index(2, 1).unwrap(), 1);
        assert!(checked_output_index(2, 2).is_err());
        assert!(checked_output_index(0, 0).is_err());
    }
}
