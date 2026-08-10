use enumflags2::{BitFlags, bitflags};
use zbus::{interface, object_server::SignalEmitter, zvariant::OwnedObjectPath};

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use zbus::zvariant::Type;

use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;

use crate::{
    clipboard::remove_clipboard_session,
    remotedesktop::{RemoteAuthorization, remove_remote_session},
    screencast::{SelectSourcesOptions, remove_cast_session},
};

pub static SESSIONS: LazyLock<Arc<Mutex<Vec<Session>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(Vec::new())));

pub async fn append_session(session: Session) {
    let mut sessions = SESSIONS.lock().await;
    sessions.push(session)
}

pub async fn remove_session(session: &Session) {
    let mut sessions = SESSIONS.lock().await;
    let Some(index) = sessions
        .iter()
        .position(|the_session| the_session.handle_path == session.handle_path)
    else {
        return;
    };
    remove_cast_session(&session.handle_path.to_string()).await;
    remove_remote_session(&session.handle_path.to_string()).await;
    remove_clipboard_session(session.handle_path.as_ref()).await;
    sessions.remove(index);
}

#[bitflags]
#[derive(Serialize_repr, Default, Deserialize_repr, PartialEq, Eq, Copy, Clone, Debug, Type)]
#[repr(u32)]
/// A bit flag for the available sources to record.
pub enum SourceType {
    #[default]
    /// A monitor.
    Monitor,
    /// A specific window
    Window,
    /// Virtual
    Virtual,
}

#[bitflags]
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Eq, Debug, Copy, Clone, Type, Default)]
#[repr(u32)]
/// A bit flag for the possible cursor modes.
pub enum CursorMode {
    #[default]
    /// The cursor is not part of the screen cast stream.
    Hidden = 1,
    /// The cursor is embedded as part of the stream buffers.
    Embedded = 2,
    /// The cursor is not part of the screen cast stream, but sent as PipeWire
    /// stream metadata.
    Metadata = 4,
}

// Remote
#[bitflags]
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Eq, Debug, Copy, Clone, Type, Default)]
#[repr(u32)]
/// A bit flag for the possible cursor modes.
pub enum DeviceType {
    #[default]
    /// The cursor is not part of the screen cast stream.
    Keyboard = 1,
    /// The cursor is embedded as part of the stream buffers.
    Pointer = 2,
    /// The cursor is not part of the screen cast stream, but sent as PipeWire
    /// stream metadata.
    TouchScreen = 4,
}

impl CursorMode {
    pub fn show_cursor(&self) -> bool {
        !matches!(self, CursorMode::Hidden)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionType {
    ScreenCast,
    Remote,
    InputCapture,
}

#[derive(Default, Serialize, Deserialize, PartialEq, Eq, Debug, Copy, Clone, Type)]
#[repr(u32)]
/// Persistence mode for a screencast session.
pub enum PersistMode {
    #[default]
    /// Do not persist.
    DoNot = 0,
    /// Persist while the application is running.
    Application = 1,
    /// Persist until explicitly revoked.
    ExplicitlyRevoked = 2,
}

#[derive(Debug, Clone)]
// TODO: when is remote?
pub struct Session {
    pub session_type: SessionType,
    pub handle_path: OwnedObjectPath,
    pub source_type: BitFlags<SourceType>,
    pub multiple: bool,
    pub cursor_mode: CursorMode,
    pub persist_mode: PersistMode,

    pub device_type: BitFlags<DeviceType>,
    pub screen_share_enabled: bool,
    pub clipboard_requested: bool,
    pub remote_restore: Option<RemoteAuthorization>,
}

impl Session {
    pub fn new<P: Into<OwnedObjectPath>>(path: P, session_type: SessionType) -> Self {
        Self {
            session_type,
            handle_path: path.into(),
            source_type: SourceType::Monitor.into(),
            multiple: false,
            cursor_mode: CursorMode::Hidden,
            persist_mode: PersistMode::DoNot,
            device_type: BitFlags::empty(),
            screen_share_enabled: false,
            clipboard_requested: false,
            remote_restore: None,
        }
    }
    pub fn set_screencast_options(&mut self, options: SelectSourcesOptions) {
        self.screen_share_enabled = true;
        if let Some(types) = options.types {
            self.source_type = types;
        }
        self.multiple = options.multiple.is_some_and(|content| content);
        if let Some(cursormode) = options.cursor_mode {
            self.cursor_mode = cursormode;
        }
        if self.session_type == SessionType::ScreenCast
            && let Some(persist_mode) = options.persist_mode
        {
            self.persist_mode = persist_mode;
        }
    }

    pub fn set_remote_options(
        &mut self,
        types: Option<BitFlags<DeviceType>>,
        persist_mode: Option<PersistMode>,
        remote_restore: Option<RemoteAuthorization>,
    ) {
        self.remote_restore = remote_restore;
        self.device_type =
            types.unwrap_or(DeviceType::Keyboard | DeviceType::Pointer | DeviceType::TouchScreen);
        if let Some(persist_mode) = persist_mode {
            self.persist_mode = persist_mode;
        }
    }
}

#[interface(name = "org.freedesktop.impl.portal.Session")]
impl Session {
    async fn close(
        &self,
        #[zbus(signal_emitter)] cxts: SignalEmitter<'_>,
        #[zbus(object_server)] server: &zbus::ObjectServer,
    ) -> zbus::fdo::Result<()> {
        server
            .remove::<Self, &OwnedObjectPath>(&self.handle_path)
            .await?;
        remove_session(self).await;
        Self::closed(&cxts, "Closed").await?;
        Ok(())
    }

    #[zbus(property, name = "version")]
    fn version(&self) -> u32 {
        2
    }

    #[zbus(signal)]
    async fn closed(signal_ctxt: &SignalEmitter<'_>, message: &str) -> zbus::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(session_type: SessionType) -> Session {
        Session::new(
            OwnedObjectPath::try_from("/org/freedesktop/portal/desktop/session/test").unwrap(),
            session_type,
        )
    }

    #[test]
    fn select_devices_defaults_to_all_devices() {
        let mut session = session(SessionType::Remote);
        assert!(session.device_type.is_empty());

        session.set_remote_options(None, None, None);
        assert_eq!(
            session.device_type,
            DeviceType::Keyboard | DeviceType::Pointer | DeviceType::TouchScreen
        );
    }

    #[test]
    fn screencast_options_do_not_override_remote_persistence() {
        let mut session = session(SessionType::Remote);
        session.set_remote_options(None, Some(PersistMode::ExplicitlyRevoked), None);
        session.set_screencast_options(SelectSourcesOptions {
            persist_mode: Some(PersistMode::DoNot),
            ..Default::default()
        });

        assert_eq!(session.persist_mode, PersistMode::ExplicitlyRevoked);
        assert!(session.screen_share_enabled);
    }
}
