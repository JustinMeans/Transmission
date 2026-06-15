import Testing
import Foundation
@testable import Transmission

/// Tests for SyncedState — a wrapper that tracks whether a value has been
/// synchronized with remote peers.
///
/// Bug: SyncedState.init set isDirty = false, meaning a freshly-created
/// state appeared "clean" even though it had NEVER been synced to any peer
/// (lastSyncedAt == nil). Sync loops that skip the work when isDirty == false
/// would therefore never push the initial value to peers.
///
/// The fix: SyncedState.init now sets isDirty = true so the first sync always
/// fires, consistent with lastSyncedAt == nil.
@Suite("SyncedState Tests")
struct StateSyncTests {

    // MARK: - Initialization

    @Test("Fresh SyncedState.isDirty is true so initial value is synced")
    func freshStateIsDirty() {
        let state = SyncedState(42)
        // A newly created state has never been synced; isDirty must be true
        // so that any sync loop will push the initial value.
        #expect(state.isDirty == true)
    }

    @Test("Fresh SyncedState.lastSyncedAt is nil")
    func freshStateLastSyncedAtNil() {
        let state = SyncedState("hello")
        #expect(state.lastSyncedAt == nil)
    }

    @Test("Fresh SyncedState.value equals the initial value")
    func freshStateValue() {
        let state = SyncedState(99)
        #expect(state.value == 99)
    }

    // MARK: - update()

    @Test("update() sets isDirty to true")
    func updateSetsDirty() {
        var state = SyncedState(0)
        state.markSynced()
        #expect(state.isDirty == false)
        state.update(1)
        #expect(state.isDirty == true)
    }

    @Test("update() replaces the value")
    func updateReplacesValue() {
        var state = SyncedState("initial")
        state.update("updated")
        #expect(state.value == "updated")
    }

    // MARK: - markSynced()

    @Test("markSynced() clears isDirty")
    func markSyncedClearsDirty() {
        var state = SyncedState(7)
        #expect(state.isDirty == true)
        state.markSynced()
        #expect(state.isDirty == false)
    }

    @Test("markSynced() records a non-nil lastSyncedAt")
    func markSyncedRecordsTimestamp() {
        var state = SyncedState(3.14)
        let before = Date()
        state.markSynced()
        let after = Date()
        let ts = state.lastSyncedAt
        #expect(ts != nil)
        #expect(ts! >= before)
        #expect(ts! <= after)
    }

    @Test("markSynced() followed by update() sets isDirty true again")
    func syncThenUpdateIsDirty() {
        var state = SyncedState(0)
        state.markSynced()
        state.update(1)
        #expect(state.isDirty == true)
    }

    @Test("markSynced() does not change the stored value")
    func markSyncedPreservesValue() {
        var state = SyncedState(42)
        state.markSynced()
        #expect(state.value == 42)
    }

    // MARK: - Sync-loop simulation

    @Test("Sync loop driven by isDirty sends initial value then goes quiet")
    func syncLoopSendsInitialValue() {
        var state = SyncedState("payload")
        var syncCount = 0

        // Simulate a sync loop: if dirty, sync and mark clean.
        if state.isDirty {
            syncCount += 1
            state.markSynced()
        }

        #expect(syncCount == 1, "Initial value must trigger exactly one sync")
        #expect(state.isDirty == false)

        // Second iteration of the loop: nothing to sync.
        if state.isDirty {
            syncCount += 1
            state.markSynced()
        }

        #expect(syncCount == 1, "No additional syncs without an update")
    }
}
