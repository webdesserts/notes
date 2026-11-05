//! Event infrastructure for vault-sync.
//!
//! Provides `SyncEvent` for debug/monitoring and `EventBus` for subscriptions.
//! Platform-specific implementations handle thread safety:
//! - Native: `Arc<EventBus>` with `RwLock` for multi-threaded Tokio runtime
//! - WASM: `Rc<EventBus>` with `RefCell` for single-threaded browser environment

use serde::Serialize;

/// Sync events emitted during sync operations for real-time monitoring.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SyncEvent {
    /// Incoming sync message received from a peer.
    MessageReceived {
        /// Protocol message type (e.g., "SyncRequest", "SyncResponse").
        #[serde(rename = "messageType")]
        message_type: String,
        /// Message size in bytes.
        size: usize,
        /// When the message was received, in milliseconds since Unix epoch.
        timestamp: f64,
    },
    /// Outgoing sync message prepared for peers.
    MessageSent {
        /// Protocol message type (e.g., "SyncRequest", "DocumentUpdate").
        #[serde(rename = "messageType")]
        message_type: String,
        /// Message size in bytes.
        size: usize,
        /// When the message was prepared, in milliseconds since Unix epoch.
        timestamp: f64,
    },
    /// Document modified by sync operation.
    DocumentUpdated {
        /// Path to the modified document.
        path: String,
        /// When the document was updated, in milliseconds since Unix epoch.
        timestamp: f64,
    },
    /// File operation (create/delete/rename) in the tree.
    FileOp {
        /// Operation type: "delete" or "rename".
        operation: String,
        /// Path affected by the operation.
        path: String,
        /// New path (for rename operations only).
        #[serde(rename = "newPath")]
        new_path: Option<String>,
        /// When the operation occurred, in milliseconds since Unix epoch.
        timestamp: f64,
    },
    /// A peer connected.
    PeerConnected {
        /// Peer's unique identifier.
        #[serde(rename = "peerId")]
        peer_id: String,
        /// When the connection completed, in milliseconds since Unix epoch.
        timestamp: f64,
    },
    /// A peer disconnected.
    PeerDisconnected {
        /// Peer's unique identifier.
        #[serde(rename = "peerId")]
        peer_id: String,
        /// When the disconnection occurred, in milliseconds since Unix epoch.
        timestamp: f64,
    },
}

// ============================================================================
// Native (multi-threaded) implementation
// ============================================================================

#[cfg(not(target_arch = "wasm32"))]
mod platform {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock, Weak};

    /// Subscription handle that unsubscribes automatically when dropped.
    ///
    /// Follows the disposer pattern: hold this value to keep receiving events,
    /// drop it (or let it go out of scope) to unsubscribe.
    pub struct Subscription {
        bus: Weak<EventBus>,
        id: usize,
    }

    impl Drop for Subscription {
        fn drop(&mut self) {
            if let Some(bus) = self.bus.upgrade() {
                bus.unsubscribe(self.id);
            }
        }
    }

    /// Event bus for publishing sync events to subscribers.
    ///
    /// Thread-safe for use in multi-threaded Tokio runtime.
    /// Wrap in `Arc` to enable subscriptions.
    pub struct EventBus {
        // Carried verbatim from the legacy sync crate; the nested handler generic is acceptable here.
        #[allow(clippy::type_complexity)]
        callbacks: RwLock<Vec<(usize, Arc<dyn Fn(SyncEvent) + Send + Sync>)>>,
        next_id: AtomicUsize,
    }

    impl Default for EventBus {
        fn default() -> Self {
            Self {
                callbacks: RwLock::new(Vec::new()),
                next_id: AtomicUsize::new(0),
            }
        }
    }

    impl EventBus {
        pub fn new() -> Self {
            Self::default()
        }

        /// Subscribe to events. Returns `Subscription` that unsubscribes on drop.
        ///
        /// Requires `self` to be wrapped in `Arc`.
        pub fn subscribe(
            self: &Arc<Self>,
            callback: impl Fn(SyncEvent) + Send + Sync + 'static,
        ) -> Subscription {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            self.callbacks
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .push((id, Arc::new(callback)));
            Subscription {
                bus: Arc::downgrade(self),
                id,
            }
        }

        fn unsubscribe(&self, id: usize) {
            // Use try_write to avoid deadlock if Drop runs during panic unwinding
            // while a read lock is held (e.g., during emit).
            if let Ok(mut guard) = self.callbacks.try_write() {
                guard.retain(|(i, _)| *i != id);
            }
        }

        /// Emit an event to all subscribers.
        pub fn emit(&self, event: SyncEvent) {
            // Clone the callback list to prevent deadlock if a callback calls subscribe.
            let callbacks: Vec<_> = self
                .callbacks
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .map(|(_, cb)| Arc::clone(cb))
                .collect();

            for callback in callbacks {
                callback(event.clone());
            }
        }
    }
}

// ============================================================================
// WASM (single-threaded) implementation
// ============================================================================

#[cfg(target_arch = "wasm32")]
mod platform {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::rc::{Rc, Weak};

    /// Subscription handle that unsubscribes automatically when dropped.
    ///
    /// Follows the disposer pattern: hold this value to keep receiving events,
    /// drop it (or let it go out of scope) to unsubscribe.
    pub struct Subscription {
        bus: Weak<EventBus>,
        id: usize,
    }

    impl Drop for Subscription {
        fn drop(&mut self) {
            if let Some(bus) = self.bus.upgrade() {
                bus.unsubscribe(self.id);
            }
        }
    }

    /// Event bus for publishing sync events to subscribers.
    ///
    /// Single-threaded for WASM browser environment.
    /// Wrap in `Rc` to enable subscriptions.
    pub struct EventBus {
        callbacks: RefCell<Vec<(usize, Rc<dyn Fn(SyncEvent)>)>>,
        next_id: Cell<usize>,
    }

    impl Default for EventBus {
        fn default() -> Self {
            Self {
                callbacks: RefCell::new(Vec::new()),
                next_id: Cell::new(0),
            }
        }
    }

    impl EventBus {
        pub fn new() -> Self {
            Self::default()
        }

        /// Subscribe to events. Returns `Subscription` that unsubscribes on drop.
        ///
        /// Requires `self` to be wrapped in `Rc`.
        pub fn subscribe(self: &Rc<Self>, callback: impl Fn(SyncEvent) + 'static) -> Subscription {
            let id = self.next_id.get();
            self.next_id.set(id + 1);
            self.callbacks.borrow_mut().push((id, Rc::new(callback)));
            Subscription {
                bus: Rc::downgrade(self),
                id,
            }
        }

        fn unsubscribe(&self, id: usize) {
            self.callbacks.borrow_mut().retain(|(i, _)| *i != id);
        }

        /// Emit an event to all subscribers.
        pub fn emit(&self, event: SyncEvent) {
            // Clone the callback list to prevent panic if a callback calls subscribe.
            let callbacks: Vec<_> = self
                .callbacks
                .borrow()
                .iter()
                .map(|(_, cb)| Rc::clone(cb))
                .collect();

            for callback in callbacks {
                callback(event.clone());
            }
        }
    }
}

pub use platform::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[cfg(not(target_arch = "wasm32"))]
    use std::sync::Arc;

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_subscribe_and_emit() {
        let bus = Arc::new(EventBus::new());
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&count);

        let _sub = bus.subscribe(move |_event| {
            count_clone.fetch_add(1, Ordering::Relaxed);
        });

        bus.emit(SyncEvent::DocumentUpdated {
            path: "test.md".into(),
            timestamp: 1000.0,
        });

        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_subscription_unsubscribes_on_drop() {
        let bus = Arc::new(EventBus::new());
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&count);

        {
            let _sub = bus.subscribe(move |_event| {
                count_clone.fetch_add(1, Ordering::Relaxed);
            });

            bus.emit(SyncEvent::DocumentUpdated {
                path: "test.md".into(),
                timestamp: 1000.0,
            });

            assert_eq!(count.load(Ordering::Relaxed), 1);
            // _sub dropped here
        }

        // After drop, callback should not be called
        bus.emit(SyncEvent::DocumentUpdated {
            path: "test2.md".into(),
            timestamp: 2000.0,
        });

        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_multiple_subscribers() {
        let bus = Arc::new(EventBus::new());
        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        let count1_clone = Arc::clone(&count1);
        let count2_clone = Arc::clone(&count2);

        let _sub1 = bus.subscribe(move |_| {
            count1_clone.fetch_add(1, Ordering::Relaxed);
        });
        let _sub2 = bus.subscribe(move |_| {
            count2_clone.fetch_add(1, Ordering::Relaxed);
        });

        bus.emit(SyncEvent::DocumentUpdated {
            path: "test.md".into(),
            timestamp: 1000.0,
        });

        assert_eq!(count1.load(Ordering::Relaxed), 1);
        assert_eq!(count2.load(Ordering::Relaxed), 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_partial_unsubscribe() {
        let bus = Arc::new(EventBus::new());
        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        let count1_clone = Arc::clone(&count1);
        let count2_clone = Arc::clone(&count2);

        let sub1 = bus.subscribe(move |_| {
            count1_clone.fetch_add(1, Ordering::Relaxed);
        });
        let _sub2 = bus.subscribe(move |_| {
            count2_clone.fetch_add(1, Ordering::Relaxed);
        });

        bus.emit(SyncEvent::DocumentUpdated {
            path: "test.md".into(),
            timestamp: 1000.0,
        });

        assert_eq!(count1.load(Ordering::Relaxed), 1);
        assert_eq!(count2.load(Ordering::Relaxed), 1);

        // Drop sub1 explicitly
        drop(sub1);

        bus.emit(SyncEvent::DocumentUpdated {
            path: "test2.md".into(),
            timestamp: 2000.0,
        });

        // Only sub2 should have incremented
        assert_eq!(count1.load(Ordering::Relaxed), 1);
        assert_eq!(count2.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_sync_event_serialization() {
        let event = SyncEvent::MessageReceived {
            message_type: "SyncRequest".into(),
            size: 1024,
            timestamp: 1234567890.0,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"type\":\"messageReceived\""));
        assert!(json.contains("\"messageType\":\"SyncRequest\""));
        assert!(json.contains("\"size\":1024"));
        assert!(json.contains("\"timestamp\":"));
    }
}
