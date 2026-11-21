using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace XDev.Outbox.EFCore
{
    public abstract class OutboxDbContext<TKey> : OutboxDbContext<TKey, OutboxDbContext<TKey>>, IOutboxDbContext<TKey> where TKey : struct
    {
        public OutboxDbContext(DbContextOptions options)
            : base(options)
        {
        }
    }

    // Event buffer: each context owns its buffer (Outbox, backed by ContextOutbox); events are persisted only by this
    // context's saves. Pooled contexts are reused, so the buffer and every piece of per-transaction/per-save state
    // below belong to one lease: EnsureCurrentLease (called at every save entry point, every transaction-participant
    // member and every public buffer method) discards them when ContextId.Lease changed. Events that were never saved
    // are therefore discarded when the context is returned to a pool (or disposed); pending messages of a caller
    // transaction left open at the end of a lease are never dispatched (EF rolls that transaction back).
    //
    // State machine for events saved inside a caller-owned transaction (owned saves commit and dispatch in one step:
    // after the commit they remove their events from the buffer, then accept changes (flag = true), then enqueue; if
    // acceptance throws, the exception propagates, nothing is enqueued and the poller delivers the committed rows).
    //
    // Every save collects the events to save before calling EF's save, i.e. before EF raises SavingChanges, runs save
    // interceptors or raises SavedChanges. An event added to the buffer while a save runs is therefore never part of
    // that save, on any path: an owned save removes from the buffer only the events it wrote (by reference and
    // multiplicity), a caller-transaction commit removes only the committed ones, and a save without events does not
    // touch the buffer, so such an event stays buffered and is written by the next save.
    //
    // A save with events inside a caller transaction runs inside the library's savepoint (__XDevOutboxSavePoint, when
    // the transaction supports savepoints and Database.AutoSavepointsEnabled is true, like EF's own savepoint). When
    // anything after its creation fails (EF's save, payload re-serialization, payload UPDATEs, completion), the save
    // rolls back to it and restores the tracker, so it leaves nothing behind and can be retried in the transaction;
    // no message becomes pending. Without a savepoint, or when that rollback fails, the writes stay in the transaction
    // and warning 41002 (CallerTransactionSaveNotUndoneEventId) tells the caller to roll back.
    //
    // A save with events inside a caller transaction records one pending message per event (_pendingOutboxMessages,
    // tied to _pendingTransactionId); its event stays in the buffer and is not saved again in that transaction.
    // Pending message states:
    //   - not accepted: saved with acceptAllChangesOnSuccess = false; its business changes are still pending in the tracker.
    //   - accepted: saved with acceptAllChangesOnSuccess = true, or any later flag = true save in the transaction
    //     (with or without events) accepted every tracked change.
    //   - unconfirmed (orthogonal): the existence check after a rollback to a savepoint failed; never dispatched in
    //     memory until a later check (next save, next savepoint rollback, committing) confirms or releases it.
    // Transitions and buffer rule:
    //   - savepoint rollback / successful check: rows that no longer exist are released (below); existing rows confirmed.
    //   - committing: unconfirmed messages are checked again; if that fails, a warning (UnconfirmedAtCommitEventId) is logged.
    //   - committed: confirmed messages are dispatched; their events leave the buffer.
    //   - ended without commit (rollback, failed commit/rollback) and stale (another transaction is current or
    //     committed, i.e. the earlier one was disposed): every pending message is released.
    //   - released: no dispatch. Accepted -> its event leaves the buffer (the changes will not be saved again).
    //     Not accepted -> its event stays buffered and is saved again with its still-pending business changes.
    // Never-saved events (buffered events with no pending message of the ended transaction: added after the last save
    // or before the transaction, kept by an earlier end, or whose save failed) stay buffered for every end, commit
    // included, with their still-pending business changes. An event therefore leaves the buffer only when it is
    // committed (caller or owned save), when its saved, accepted message is released (above), by Outbox.ClearEvents(),
    // or at the end of the context/pool lease.
    public abstract class OutboxDbContext<TKey, TContext> : DbContext, IOutboxTransactionParticipant<TKey> where TKey : struct where TContext : DbContext, IOutboxDbContext<TKey>
    {
        // Upper bound of ids per existence query after a rollback to a savepoint (keeps parameter counts low).
        private const int ExistenceQueryBatchSize = 100;

        /// <summary>
        /// Event id of the warning logged (category <c>Microsoft.EntityFrameworkCore.Database.Transaction</c>) when,
        /// just before a caller transaction commits, the existence of pending outbox rows cannot be confirmed after a
        /// failed check following a rollback to a savepoint: id 41001, name
        /// <c>XDev.Outbox.EFCore.UnconfirmedPendingMessagesAtCommit</c>. The commit proceeds; those messages are not
        /// dispatched in memory and committed rows are delivered by the outbox poller.
        /// </summary>
        private static readonly EventId UnconfirmedAtCommitEventId = new EventId(41001, "XDev.Outbox.EFCore.UnconfirmedPendingMessagesAtCommit");

        /// <summary>
        /// Event id of the warning logged (category <c>Microsoft.EntityFrameworkCore.Database.Transaction</c>) when a
        /// save with events failed inside a caller-owned transaction after EF had written its changes, and those
        /// changes could not be undone: the rollback to the library's savepoint failed, or no savepoint was used
        /// (the transaction does not support savepoints, e.g. SQL Server with MARS, or
        /// <c>Database.AutoSavepointsEnabled</c> is false). Id 41002, name
        /// <c>XDev.Outbox.EFCore.CallerTransactionSaveNotUndone</c>. The original exception is still thrown; the
        /// caller must roll back the transaction instead of retrying the save in it.
        /// </summary>
        private static readonly EventId CallerTransactionSaveNotUndoneEventId = new EventId(41002, "XDev.Outbox.EFCore.CallerTransactionSaveNotUndone");

        // Savepoint around a save with events in a caller-owned transaction (distinct from EF's "__EFSavePoint"; SQL
        // Server limits savepoint names to 32 characters).
        private const string OutboxSavepointName = "__XDevOutboxSavePoint";

        // Messages saved inside a caller-owned transaction; dispatched by the interceptor when that transaction commits.
        private readonly List<SavedOutboxMessage> _pendingOutboxMessages = new List<SavedOutboxMessage>();
        private Guid? _pendingTransactionId;
        private bool _outboxSaveInProgress;
        // > 0 while one of the SaveChanges overrides runs; savepoints EF creates and rolls back inside a save are ignored.
        private int _saveDepth;
        // The buffer behind Outbox; internal code uses its list directly after EnsureCurrentLease.
        private readonly ContextOutbox _outbox;
        // ContextId.Lease the state above belongs to (always 0 for contexts that are not pooled).
        private int _stateLease;

        public DbSet<OutboxMessage<TKey>> OutboxMessages { get; set; }

        public OutboxDbContext(DbContextOptions options)
            : base(options)
        {
            _outbox = new ContextOutbox(EnsureCurrentLease);
        }

        /// <summary>
        /// The events to publish with this context's next save. Events added here are written to the outbox table by
        /// this context's <c>SaveChanges</c>/<c>SaveChangesAsync</c>, in the same transaction as the business changes,
        /// and are dispatched to subscribers after that transaction commits.
        /// </summary>
        /// <remarks>
        /// The buffer belongs to this context instance (never null; the same instance for the life of the context) and
        /// is not thread-safe, like the context. Only this context's saves persist its events: events added to another
        /// context are not saved by this one. Events that were never saved are discarded when the context is disposed
        /// or returned to a pool (<c>AddDbContextPool</c>, <c>AddPooledDbContextFactory</c>); a pooled context starts
        /// every lease with an empty buffer.
        /// <para>
        /// A save writes the events that were buffered when it was called. Events added while a save runs (for example
        /// by a <c>SavingChanges</c> or <c>SavedChanges</c> event handler, or by a save interceptor that collects domain
        /// events) are not part of that save: they stay buffered and are written by this context's next save, not
        /// atomically with the save during which they were added (after an owned save, the next save runs in a later
        /// transaction). To write an event atomically with the business changes it describes, add it before
        /// calling <c>SaveChanges</c>/<c>SaveChangesAsync</c>.
        /// </para>
        /// </remarks>
        public IOutbox Outbox => _outbox;

        protected override void OnModelCreating(ModelBuilder builder)
        {
            var internalServiceProvider = this.GetInfrastructure();
            var outboxOptions = (IOptions<OutboxDbContextConfiguration>)(internalServiceProvider.GetService(typeof(IOptions<OutboxDbContextConfiguration>))
                ?? internalServiceProvider.GetService<IDbContextOptions>()
                    ?.Extensions.OfType<CoreOptionsExtension>().FirstOrDefault()
                    ?.ApplicationServiceProvider
                    ?.GetService(typeof(IOptions<OutboxDbContextConfiguration>)));

            builder.ApplyConfiguration(new OutboxMessageConfiguration<TKey>(outboxOptions));
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);
        }

        public override int SaveChanges(bool acceptAllChangesOnSuccess)
        {
            EnsureCurrentLease();
            _saveDepth++;
            try
            {
                return SaveChangesCore(acceptAllChangesOnSuccess);
            }
            finally
            {
                _saveDepth--;
            }
        }

        public override async Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default)
        {
            EnsureCurrentLease();
            _saveDepth++;
            try
            {
                return await SaveChangesCoreAsync(acceptAllChangesOnSuccess, cancellationToken);
            }
            finally
            {
                _saveDepth--;
            }
        }

        private int SaveChangesCore(bool acceptAllChangesOnSuccess)
        {
            var messageQueue = this.GetService<IMessageQueue<TKey>>();
            var timeProvider = this.GetService<TimeProvider>();

            if (HasUnconfirmedPendingMessages(Database.CurrentTransaction?.TransactionId))
            {
                ResolvePendingMessages(QueryExistingPendingIds());
            }

            var events = GetEventsToSave();
            if (events.Count == 0)
            {
                var plainResult = base.SaveChanges(acceptAllChangesOnSuccess);
                if (acceptAllChangesOnSuccess) MarkPendingChangesAccepted();
                return plainResult;
            }

            // Before any command, transaction or savepoint: a name the column cannot hold fails the whole save.
            ThrowIfEventTypeNameTooLong(events);

            var callerTransaction = Database.CurrentTransaction;
            if (callerTransaction != null)
            {
                var useSavepoint = UseOutboxSavepoint(callerTransaction);
                var ambientAttempt = BeginAttempt(events, timeProvider);
                var savepointCreated = false;
                OutboxSaveResult ambient;
                try
                {
                    if (useSavepoint)
                    {
                        callerTransaction.CreateSavepoint(OutboxSavepointName);
                        savepointCreated = true;
                    }
                    ambient = RunAttempt(ambientAttempt, acceptAllChangesOnSuccess);
                    if (savepointCreated)
                    {
                        callerTransaction.ReleaseSavepoint(OutboxSavepointName);
                    }
                }
                catch (Exception ex)
                {
                    UndoFailedAmbientAttempt(callerTransaction, savepointCreated, ambientAttempt);
                    RecoverFailedAttempt(ambientAttempt, ex);
                    throw;
                }
                return CompleteAmbientSave(ambient, acceptAllChangesOnSuccess);
            }

            OutboxSaveResult saved;
            _outboxSaveInProgress = true;
            try
            {
                saved = Database.CreateExecutionStrategy().Execute(() =>
                {
                    var attempt = BeginAttempt(events, timeProvider);
                    try
                    {
                        using var transaction = Database.BeginTransaction(IsolationLevel.ReadCommitted);
                        var result = RunAttempt(attempt, acceptAllChangesOnSuccess);
                        transaction.Commit();
                        return result;
                    }
                    catch (Exception ex)
                    {
                        RecoverFailedAttempt(attempt, ex);
                        throw;
                    }
                });
            }
            finally
            {
                _outboxSaveInProgress = false;
            }

            CompleteOwnedSave(saved.Messages, acceptAllChangesOnSuccess);
            foreach (var item in saved.Messages)
            {
                messageQueue.EnqueueAsync(item.Message).GetAwaiter().GetResult();
            }

            return saved.Result;
        }

        private async Task<int> SaveChangesCoreAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken)
        {
            var messageQueue = this.GetService<IMessageQueue<TKey>>();
            var timeProvider = this.GetService<TimeProvider>();

            if (HasUnconfirmedPendingMessages(Database.CurrentTransaction?.TransactionId))
            {
                ResolvePendingMessages(await QueryExistingPendingIdsAsync(cancellationToken));
            }

            var events = GetEventsToSave();
            if (events.Count == 0)
            {
                var plainResult = await base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
                if (acceptAllChangesOnSuccess) MarkPendingChangesAccepted();
                return plainResult;
            }

            // Before any command, transaction or savepoint: a name the column cannot hold fails the whole save.
            ThrowIfEventTypeNameTooLong(events);

            var callerTransaction = Database.CurrentTransaction;
            if (callerTransaction != null)
            {
                var useSavepoint = UseOutboxSavepoint(callerTransaction);
                var ambientAttempt = BeginAttempt(events, timeProvider);
                var savepointCreated = false;
                OutboxSaveResult ambient;
                try
                {
                    if (useSavepoint)
                    {
                        await callerTransaction.CreateSavepointAsync(OutboxSavepointName, cancellationToken);
                        savepointCreated = true;
                    }
                    ambient = await RunAttemptAsync(ambientAttempt, acceptAllChangesOnSuccess, cancellationToken);
                    if (savepointCreated)
                    {
                        await callerTransaction.ReleaseSavepointAsync(OutboxSavepointName, cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    await UndoFailedAmbientAttemptAsync(callerTransaction, savepointCreated, ambientAttempt);
                    RecoverFailedAttempt(ambientAttempt, ex);
                    throw;
                }
                return CompleteAmbientSave(ambient, acceptAllChangesOnSuccess);
            }

            OutboxSaveResult saved;
            _outboxSaveInProgress = true;
            try
            {
                saved = await Database.CreateExecutionStrategy().ExecuteAsync(async ct =>
                {
                    var attempt = BeginAttempt(events, timeProvider);
                    try
                    {
                        await using var transaction = await Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
                        var result = await RunAttemptAsync(attempt, acceptAllChangesOnSuccess, ct);
                        await transaction.CommitAsync(ct);
                        return result;
                    }
                    catch (Exception ex)
                    {
                        RecoverFailedAttempt(attempt, ex);
                        throw;
                    }
                }, cancellationToken);
            }
            finally
            {
                _outboxSaveInProgress = false;
            }

            CompleteOwnedSave(saved.Messages, acceptAllChangesOnSuccess);
            foreach (var item in saved.Messages)
            {
                await messageQueue.EnqueueAsync(item.Message, cancellationToken);
            }

            return saved.Result;
        }

        /// <summary>
        /// A pooled context is reused across leases (<see cref="DbContextId.Lease"/> changes for every lease); the buffer
        /// and all per-transaction/per-save state of an earlier lease are discarded at the first use in a new lease.
        /// Pending messages of an earlier lease are never dispatched: a caller transaction left open when the context
        /// went back to the pool is rolled back by EF, without an interceptor event. Contexts that are not pooled
        /// keep lease 0, so this never discards anything for them.
        /// </summary>
        private void EnsureCurrentLease()
        {
            var lease = ContextId.Lease;
            if (lease == _stateLease) return;

            _stateLease = lease;
            _outbox.Events.Clear();
            _pendingOutboxMessages.Clear();
            _pendingTransactionId = null;
            _outboxSaveInProgress = false;
            _saveDepth = 0;
        }

        bool IOutboxTransactionParticipant<TKey>.IsOutboxSaveInProgress
        {
            get
            {
                EnsureCurrentLease();
                return _outboxSaveInProgress;
            }
        }

        void IOutboxTransactionParticipant<TKey>.TransactionCommitting(Guid transactionId)
        {
            EnsureCurrentLease();
            if (!HasUnconfirmedPendingMessages(transactionId)) return;

            HashSet<TKey> existing;
            try
            {
                existing = QueryExistingPendingIds();
            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                LogUnconfirmedPendingMessages(ex);
                return;
            }
            ResolvePendingMessages(existing);
        }

        async Task IOutboxTransactionParticipant<TKey>.TransactionCommittingAsync(Guid transactionId, CancellationToken cancellationToken)
        {
            EnsureCurrentLease();
            if (!HasUnconfirmedPendingMessages(transactionId)) return;

            HashSet<TKey> existing;
            try
            {
                existing = await QueryExistingPendingIdsAsync(cancellationToken);
            }
            catch (Exception ex) when (!(ex is OperationCanceledException))
            {
                LogUnconfirmedPendingMessages(ex);
                return;
            }
            ResolvePendingMessages(existing);
        }

        IReadOnlyList<OutboxMessage<TKey>> IOutboxTransactionParticipant<TKey>.TransactionCommitted(Guid transactionId)
        {
            EnsureCurrentLease();
            var isCurrent = _pendingTransactionId == transactionId;
            var committed = isCurrent ? _pendingOutboxMessages.ToList() : new List<SavedOutboxMessage>();
            // Only one transaction can be current per context, so messages pending for another id belong to a
            // transaction that ended without a commit (disposed); they follow the rule for an uncommitted end.
            var stale = isCurrent ? new List<SavedOutboxMessage>() : _pendingOutboxMessages.ToList();

            // Events never saved in the committed transaction stay buffered with their still-pending business changes.
            UpdateBuffer(released: stale, committed: committed);
            _pendingOutboxMessages.Clear();
            _pendingTransactionId = null;

            // Unconfirmed messages are not dispatched in memory: if their row committed, the outbox poller delivers it.
            return committed.Where(x => !x.Unconfirmed).Select(x => x.Message).ToList();
        }

        void IOutboxTransactionParticipant<TKey>.EndTransactionWithoutCommit(Guid transactionId)
        {
            EnsureCurrentLease();
            // Only one transaction can be current per context, so anything pending for another id has ended too.
            // Never-saved events stay buffered with their still-pending business changes, as when the transaction is
            // disposed without a rollback (stale detection at the next save).
            ReleasePendingOutboxMessages(_pendingOutboxMessages.ToList());
        }

        void IOutboxTransactionParticipant<TKey>.RolledBackToSavepoint(Guid transactionId)
        {
            EnsureCurrentLease();
            if (!ShouldCheckSavepointRollback(transactionId)) return;

            HashSet<TKey> existing;
            try
            {
                existing = QueryExistingPendingIds();
            }
            catch
            {
                MarkPendingMessagesUnconfirmed();
                throw;
            }
            ResolvePendingMessages(existing);
        }

        async Task IOutboxTransactionParticipant<TKey>.RolledBackToSavepointAsync(Guid transactionId, CancellationToken cancellationToken)
        {
            EnsureCurrentLease();
            if (!ShouldCheckSavepointRollback(transactionId)) return;

            HashSet<TKey> existing;
            try
            {
                existing = await QueryExistingPendingIdsAsync(cancellationToken);
            }
            catch
            {
                MarkPendingMessagesUnconfirmed();
                throw;
            }
            ResolvePendingMessages(existing);
        }

        private bool ShouldCheckSavepointRollback(Guid transactionId)
        {
            // Savepoints rolled back inside a save (EF's own, and the library's savepoint after a failed save with
            // events) were created inside that save, so they never cover messages saved by an earlier save.
            return _saveDepth == 0 && _pendingTransactionId == transactionId && _pendingOutboxMessages.Count > 0;
        }

        private bool HasUnconfirmedPendingMessages(Guid? transactionId)
        {
            return transactionId != null && _pendingTransactionId == transactionId && _pendingOutboxMessages.Any(x => x.Unconfirmed);
        }

        /// <summary>
        /// The check after a rollback to a savepoint failed, so it is unknown which pending rows the rollback undid.
        /// The messages stay pending (their events are not saved again in this transaction) but are not dispatched in
        /// memory until a later check inside the transaction (next save, next rollback to a savepoint, or just before
        /// commit) confirms them.
        /// </summary>
        private void MarkPendingMessagesUnconfirmed()
        {
            foreach (var message in _pendingOutboxMessages)
            {
                message.Unconfirmed = true;
            }
        }

        /// <summary>A save with acceptAllChangesOnSuccess = true accepted every tracked change, including those of earlier saves.</summary>
        private void MarkPendingChangesAccepted()
        {
            foreach (var message in _pendingOutboxMessages)
            {
                message.AcceptedChanges = true;
            }
        }

        private void LogUnconfirmedPendingMessages(Exception exception)
        {
            // The commit is not blocked: committed rows of unconfirmed messages are delivered by the outbox poller.
            var logger = this.GetService<IDiagnosticsLogger<DbLoggerCategory.Database.Transaction>>();
            logger?.Logger.LogWarning(
                UnconfirmedAtCommitEventId,
                exception,
                "Could not confirm {Count} pending outbox message(s) before commit; they are not dispatched in memory and are delivered by the outbox poller if committed.",
                _pendingOutboxMessages.Count(x => x.Unconfirmed));
        }

        /// <summary>
        /// Whether a save with events in the caller's transaction is wrapped in the library's savepoint: under the same
        /// conditions as EF's own savepoint around a save (the transaction supports savepoints and
        /// <c>Database.AutoSavepointsEnabled</c> is true). AutoSavepointsEnabled is read first because reading
        /// SupportsSavepoints on SQL Server with MARS logs a warning.
        /// </summary>
        private bool UseOutboxSavepoint(IDbContextTransaction transaction)
        {
            return Database.AutoSavepointsEnabled && transaction.SupportsSavepoints;
        }

        /// <summary>
        /// Undoes, inside the caller's transaction, what a failed save with events wrote, so that the save can be
        /// retried in the same transaction: rolls back to the library's savepoint (created before EF's save) and
        /// releases it. The rollback happens inside the save (_saveDepth &gt; 0), so the transaction interceptor does
        /// not check pending messages of earlier saves (the savepoint postdates them). When the rollback fails, or
        /// no savepoint was used and EF's save had already written, the writes stay in the transaction: that error is
        /// not thrown (EF logged it; the caller gets the original exception) and warning 41002 is logged.
        /// </summary>
        private void UndoFailedAmbientAttempt(IDbContextTransaction transaction, bool savepointCreated, SaveAttempt attempt)
        {
            if (!savepointCreated)
            {
                if (attempt.BaseSaveCompleted) LogCallerTransactionSaveNotUndone(null, savepointUsed: false);
                return;
            }

            try
            {
                transaction.RollbackToSavepoint(OutboxSavepointName);
            }
            catch (Exception rollbackError)
            {
                LogCallerTransactionSaveNotUndone(rollbackError, savepointUsed: true);
                return;
            }

            try
            {
                transaction.ReleaseSavepoint(OutboxSavepointName);
            }
            catch (Exception)
            {
                // Best effort: the writes are already undone and EF logged the failure; a savepoint left in the
                // transaction changes nothing for the caller.
            }
        }

        /// <summary>
        /// Async counterpart of <see cref="UndoFailedAmbientAttempt"/>. The caller's token is not used: a cancelled save
        /// must still be undone.
        /// </summary>
        private async Task UndoFailedAmbientAttemptAsync(IDbContextTransaction transaction, bool savepointCreated, SaveAttempt attempt)
        {
            if (!savepointCreated)
            {
                if (attempt.BaseSaveCompleted) LogCallerTransactionSaveNotUndone(null, savepointUsed: false);
                return;
            }

            try
            {
                await transaction.RollbackToSavepointAsync(OutboxSavepointName, CancellationToken.None);
            }
            catch (Exception rollbackError)
            {
                LogCallerTransactionSaveNotUndone(rollbackError, savepointUsed: true);
                return;
            }

            try
            {
                await transaction.ReleaseSavepointAsync(OutboxSavepointName, CancellationToken.None);
            }
            catch (Exception)
            {
                // Best effort: the writes are already undone and EF logged the failure; a savepoint left in the
                // transaction changes nothing for the caller.
            }
        }

        private void LogCallerTransactionSaveNotUndone(Exception rollbackError, bool savepointUsed)
        {
            var logger = this.GetService<IDiagnosticsLogger<DbLoggerCategory.Database.Transaction>>();
            logger?.Logger.LogWarning(
                CallerTransactionSaveNotUndoneEventId,
                rollbackError,
                "A save with outbox events failed inside a transaction owned by the application after its changes were written, and they could not be undone ({Reason}). Roll back the transaction; do not retry the save in it.",
                savepointUsed
                    ? "the rollback to the outbox savepoint failed"
                    : "no savepoint is used: the transaction does not support savepoints or Database.AutoSavepointsEnabled is false");
        }

        private HashSet<TKey> QueryExistingPendingIds()
        {
            var existing = new HashSet<TKey>();
            foreach (var query in ExistingOutboxIdQueries())
            {
                existing.UnionWith(query.ToList());
            }
            return existing;
        }

        private async Task<HashSet<TKey>> QueryExistingPendingIdsAsync(CancellationToken cancellationToken)
        {
            var existing = new HashSet<TKey>();
            foreach (var query in ExistingOutboxIdQueries())
            {
                existing.UnionWith(await query.ToListAsync(cancellationToken));
            }
            return existing;
        }

        /// <summary>
        /// EF reports savepoint operations without the savepoint name, so the rows that a rollback to a savepoint
        /// undid are found by checking, inside the transaction, which pending outbox rows still exist.
        /// </summary>
        private IEnumerable<IQueryable<TKey>> ExistingOutboxIdQueries()
        {
            var ids = _pendingOutboxMessages.Select(x => x.Message.Id).Distinct().ToList();
            for (var offset = 0; offset < ids.Count; offset += ExistenceQueryBatchSize)
            {
                var message = Expression.Parameter(typeof(OutboxMessage<TKey>), "m");
                var idMember = Expression.Property(message, nameof(OutboxMessage<TKey>.Id));
                Expression predicate = null;
                foreach (var id in ids.Skip(offset).Take(ExistenceQueryBatchSize))
                {
                    // A member access on a closure-like holder makes EF send the id as a parameter.
                    var value = Expression.Property(Expression.Constant(new IdHolder(id)), nameof(IdHolder.Value));
                    var equal = IdEquals(idMember, value);
                    predicate = predicate == null ? equal : Expression.OrElse(predicate, equal);
                }

                yield return Set<OutboxMessage<TKey>>()
                    .IgnoreQueryFilters()
                    .AsNoTracking()
                    .Where(Expression.Lambda<Func<OutboxMessage<TKey>, bool>>(predicate, message))
                    .Select(x => x.Id);
            }
        }

        /// <summary>
        /// <c>==</c> for key types that define it (primitives, enums, Guid, ...); otherwise the typed or
        /// <see cref="object.Equals(object)"/> method, which EF also translates to an equality comparison.
        /// </summary>
        private static Expression IdEquals(Expression left, Expression right)
        {
            var type = typeof(TKey);
            if (type.IsPrimitive || type.IsEnum || type.GetMethod("op_Equality", new[] { type, type }) != null)
            {
                return Expression.Equal(left, right);
            }

            var typedEquals = type.GetMethod(nameof(Equals), new[] { type });
            return typedEquals != null
                ? Expression.Call(left, typedEquals, right)
                : Expression.Call(
                    Expression.Convert(left, typeof(object)),
                    typeof(object).GetMethod(nameof(Equals), new[] { typeof(object) }),
                    Expression.Convert(right, typeof(object)));
        }

        /// <summary>
        /// Applies an existence check made inside the transaction: messages whose row no longer exists were undone by
        /// a rollback to a savepoint and are released; the others are confirmed (an undone row never comes back, so a
        /// row that exists now was not undone by an earlier rollback).
        /// </summary>
        private void ResolvePendingMessages(HashSet<TKey> existingIds)
        {
            var rolledBack = new List<SavedOutboxMessage>();
            foreach (var message in _pendingOutboxMessages)
            {
                if (existingIds.Contains(message.Message.Id))
                {
                    message.Unconfirmed = false;
                }
                else
                {
                    rolledBack.Add(message);
                }
            }

            if (rolledBack.Count > 0)
            {
                ReleasePendingOutboxMessages(rolledBack);
            }
        }

        /// <summary>
        /// Forgets pending messages whose rows will never be committed, without dispatching them, and updates the
        /// buffer as described in <see cref="UpdateBuffer"/>.
        /// </summary>
        private void ReleasePendingOutboxMessages(IReadOnlyList<SavedOutboxMessage> released)
        {
            UpdateBuffer(released, Array.Empty<SavedOutboxMessage>());

            foreach (var message in released)
            {
                _pendingOutboxMessages.Remove(message);
            }
            if (_pendingOutboxMessages.Count == 0)
            {
                _pendingTransactionId = null;
            }
        }

        /// <summary>
        /// Rebuilds the event buffer (<see cref="Outbox"/>), order preserved; events are matched to messages by reference and
        /// multiplicity. Events of <paramref name="released"/> messages (rows that will never be committed) follow the
        /// business changes they were saved with: when those changes are still pending in the tracker (saved with
        /// acceptAllChangesOnSuccess = false and not accepted by a later save) the event stays buffered and is saved
        /// again with them; when they were accepted they will not be saved again, so the event is removed.
        /// Events of <paramref name="committed"/> messages are removed. Events of the other pending messages are kept
        /// (their transaction decides). All other events are always kept (every end of a transaction, commit
        /// included): never saved (no pending message, e.g. added after the last save or before the transaction, or
        /// their save failed), or released earlier with their business changes still pending.
        /// </summary>
        private void UpdateBuffer(IReadOnlyList<SavedOutboxMessage> released, IReadOnlyList<SavedOutboxMessage> committed)
        {
            var events = _outbox.Events;
            var buffered = events.ToList();
            var unmatchedReleased = released.ToList();
            var unmatchedCommitted = committed.ToList();
            var unmatchedRemaining = _pendingOutboxMessages.Except(released).Except(committed).ToList();

            var keep = new List<object>(buffered.Count);
            foreach (var @event in buffered)
            {
                var index = unmatchedReleased.FindIndex(x => ReferenceEquals(x.Event, @event));
                if (index >= 0)
                {
                    var message = unmatchedReleased[index];
                    unmatchedReleased.RemoveAt(index);
                    if (!message.AcceptedChanges)
                    {
                        keep.Add(@event);
                    }
                    continue;
                }

                index = unmatchedCommitted.FindIndex(x => ReferenceEquals(x.Event, @event));
                if (index >= 0)
                {
                    unmatchedCommitted.RemoveAt(index);
                    continue;
                }

                index = unmatchedRemaining.FindIndex(x => ReferenceEquals(x.Event, @event));
                if (index >= 0)
                {
                    // Still saved in the current transaction; the commit or rollback decides.
                    unmatchedRemaining.RemoveAt(index);
                    keep.Add(@event);
                    continue;
                }

                // Never saved in the ended transaction, or released earlier with its business changes still pending:
                // stays buffered and is saved later with them.
                keep.Add(@event);
            }

            if (keep.Count != buffered.Count)
            {
                events.Clear();
                events.AddRange(keep);
            }
        }

        /// <summary>
        /// Buffered events that still need an outbox row: pending messages of a transaction that is no longer current
        /// (disposed without commit or rollback) are released, and events already saved in the current transaction
        /// are skipped.
        /// </summary>
        private IReadOnlyList<object> GetEventsToSave()
        {
            if (_pendingTransactionId != null && _pendingTransactionId != Database.CurrentTransaction?.TransactionId)
            {
                ReleasePendingOutboxMessages(_pendingOutboxMessages.ToList());
            }

            var events = _outbox.Events;
            if (events.Count == 0) return Array.Empty<object>();

            var pending = _pendingOutboxMessages.ToList();
            var toSave = new List<object>();
            foreach (var @event in events)
            {
                var index = pending.FindIndex(p => ReferenceEquals(p.Event, @event));
                if (index >= 0)
                {
                    pending.RemoveAt(index);
                    continue;
                }
                toSave.Add(@event);
            }
            return toSave;
        }

        private int CompleteAmbientSave(OutboxSaveResult saved, bool acceptAllChangesOnSuccess)
        {
            var transactionId = Database.CurrentTransaction.TransactionId;
            if (_pendingTransactionId != transactionId)
            {
                // First save with events in this transaction: _pendingTransactionId is null here, because
                // GetEventsToSave already released anything pending for another transaction (so Clear is a no-op).
                _pendingOutboxMessages.Clear();
                _pendingTransactionId = transactionId;
            }
            _pendingOutboxMessages.AddRange(saved.Messages);

            if (acceptAllChangesOnSuccess)
            {
                ChangeTracker.AcceptAllChanges();
                MarkPendingChangesAccepted();
            }

            return saved.Result;
        }

        /// <summary>
        /// After an owned save committed: removes from the buffer only the events this save wrote (their original
        /// instances, by reference and multiplicity). Events added to the buffer while the save ran (a SavingChanges or
        /// SavedChanges handler, a save interceptor) were not part of it, because the events to save were collected
        /// before EF raised any save event; they stay buffered and the next save writes them, as on the
        /// caller-transaction and no-events paths.
        /// <para>
        /// The buffer is updated before <c>AcceptAllChanges</c> (flag = true) runs, because the save has already
        /// committed: if acceptance throws (e.g. a <c>ChangeTracker.StateChanged</c> handler), the exception propagates
        /// unchanged, as with plain EF, and the committed events are not written again by a later save. The caller then
        /// skips the in-memory enqueue; the outbox poller delivers the committed rows after their LockExpiresAt.
        /// </para>
        /// </summary>
        private void CompleteOwnedSave(IReadOnlyList<SavedOutboxMessage> saved, bool acceptAllChangesOnSuccess)
        {
            var events = _outbox.Events;
            foreach (var message in saved)
            {
                var index = events.FindIndex(x => ReferenceEquals(x, message.Event));
                if (index >= 0) events.RemoveAt(index);
            }

            if (acceptAllChangesOnSuccess)
            {
                ChangeTracker.AcceptAllChanges();
            }
        }

        private SaveAttempt BeginAttempt(IReadOnlyList<object> events, TimeProvider timeProvider)
        {
            var businessEntries = CaptureBusinessEntries();
            var generatedValues = CaptureGeneratedValues(businessEntries);
            var outboxEntries = AddOutboxEntries(events, timeProvider);
            return new SaveAttempt(businessEntries, generatedValues, outboxEntries);
        }

        private OutboxSaveResult RunAttempt(SaveAttempt attempt, bool acceptAllChangesOnSuccess)
        {
            var result = base.SaveChanges(false);
            attempt.BaseSaveCompleted = true;

            var patches = BuildPayloadPatches(attempt, acceptAllChangesOnSuccess);
            if (patches.Any(x => x.Changed))
            {
                var sql = BuildPayloadUpdateSql();
                foreach (var patch in patches.Where(x => x.Changed))
                {
                    Database.ExecuteSqlRaw(sql, patch.Payload, patch.ProviderId);
                }
            }

            return new OutboxSaveResult(result - attempt.OutboxEntries.Count, CompleteOutboxEntries(attempt.OutboxEntries, patches, acceptAllChangesOnSuccess));
        }

        private async Task<OutboxSaveResult> RunAttemptAsync(SaveAttempt attempt, bool acceptAllChangesOnSuccess, CancellationToken cancellationToken)
        {
            var result = await base.SaveChangesAsync(false, cancellationToken);
            attempt.BaseSaveCompleted = true;

            var patches = BuildPayloadPatches(attempt, acceptAllChangesOnSuccess);
            if (patches.Any(x => x.Changed))
            {
                var sql = BuildPayloadUpdateSql();
                foreach (var patch in patches.Where(x => x.Changed))
                {
                    await Database.ExecuteSqlRawAsync(sql, new[] { patch.Payload, patch.ProviderId }, cancellationToken);
                }
            }

            return new OutboxSaveResult(result - attempt.OutboxEntries.Count, CompleteOutboxEntries(attempt.OutboxEntries, patches, acceptAllChangesOnSuccess));
        }

        /// <summary>
        /// Puts the tracker back to its state before the failed attempt so the next attempt (execution-strategy retry
        /// or a later save by the application) inserts everything again with fresh store-generated values: the
        /// attempt's outbox entries are detached and the generated values captured before the attempt (temporary
        /// keys/foreign keys, unset store-generated values) are restored. Every step is attempted; when a step fails
        /// the original exception and the recovery failures are thrown together.
        /// When EF's own save fails after an earlier command of it ran, EF puts the temporary key back on the entries
        /// whose store-generated key it discards, but its identity map keeps them under the discarded key until
        /// changes are detected. Detaching removes an entry under its current (temporary) key, so the key change of
        /// each outbox entry is detected first (explicitly: AutoDetectChangesEnabled may be false); otherwise the
        /// discarded key stays in the identity map and a retry that gets the same key (SQLite reuses rowids) fails.
        /// </summary>
        private static void RecoverFailedAttempt(SaveAttempt attempt, Exception failure)
        {
            var errors = new List<Exception>();
            foreach (var entry in attempt.OutboxEntries)
            {
                if (entry.State == EntityState.Detached) continue;

                try
                {
                    entry.DetectChanges();
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }

                try
                {
                    entry.State = EntityState.Detached;
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }
            }

            // Setter-only properties left projected for an accepting save get their pre-save value back first.
            errors.AddRange(RestoreClrValues(attempt.DeferredClrRestores));
            attempt.DeferredClrRestores.Clear();

            RestoreGeneratedValues(attempt.GeneratedValues, errors);

            if (errors.Count > 0)
            {
                throw new AggregateException(new[] { failure }.Concat(errors));
            }
        }

        private IReadOnlyList<EntityEntry> CaptureBusinessEntries()
        {
            return ChangeTracker.Entries()
                .Where(x => (x.State == EntityState.Added || x.State == EntityState.Modified) && !(x.Entity is OutboxMessage<TKey>))
                .ToList();
        }

        /// <summary>
        /// A successful <c>SaveChanges(false)</c> replaces temporary keys and propagated foreign keys with the
        /// store-generated values and marks them as no longer temporary, and EF does not undo that when the
        /// surrounding transaction later fails. The values that the next insert needs are captured here.
        /// </summary>
        private static IReadOnlyList<GeneratedValueSnapshot> CaptureGeneratedValues(IReadOnlyList<EntityEntry> businessEntries)
        {
            var snapshots = new List<GeneratedValueSnapshot>();
            foreach (var entry in businessEntries)
            {
                if (entry.State != EntityState.Added) continue;

                foreach (var property in entry.Properties)
                {
                    var metadata = property.Metadata;
                    var value = property.CurrentValue;
                    var isTemporary = property.IsTemporary;
                    if (!isTemporary
                        && !((metadata.ValueGenerated != ValueGenerated.Never || metadata.IsForeignKey())
                            && metadata.GetValueComparer().Equals(value, metadata.Sentinel)))
                    {
                        continue;
                    }

                    // Temporary values live in the tracker, not on the entity object; its CLR value is restored too.
                    var member = GetClrMember(metadata);
                    snapshots.Add(new GeneratedValueSnapshot(property, value, isTemporary, member, member == null ? null : GetClrValue(member, entry.Entity)));
                }
            }
            return snapshots;
        }

        private static void RestoreGeneratedValues(IReadOnlyList<GeneratedValueSnapshot> snapshots, List<Exception> errors)
        {
            foreach (var snapshot in snapshots)
            {
                try
                {
                    var property = snapshot.Property;
                    if (property.EntityEntry.State != EntityState.Added) continue;

                    if (!property.Metadata.GetValueComparer().Equals(property.CurrentValue, snapshot.Value))
                    {
                        property.CurrentValue = snapshot.Value;
                    }
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }
            }

            foreach (var snapshot in snapshots)
            {
                try
                {
                    var property = snapshot.Property;
                    if (property.EntityEntry.State != EntityState.Added) continue;

                    if (snapshot.WasTemporary && !property.IsTemporary)
                    {
                        property.IsTemporary = true;
                    }
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }
            }

            // Setting a value (or marking it temporary) also writes it to the entity object; the tracker keeps the
            // temporary value, so the object gets its previous value back: through the backing field when EF knows
            // one, otherwise through the property setter. Properties with neither keep the value EF wrote.
            foreach (var snapshot in snapshots)
            {
                try
                {
                    var property = snapshot.Property;
                    if (snapshot.Member == null || property.EntityEntry.State != EntityState.Added) continue;

                    var entity = property.EntityEntry.Entity;
                    if (!Equals(GetClrValue(snapshot.Member, entity), snapshot.ClrValue))
                    {
                        SetClrValue(snapshot.Member, entity, snapshot.ClrValue);
                    }
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }
            }
        }

        private IReadOnlyList<EntityEntry<OutboxMessage<TKey>>> AddOutboxEntries(IReadOnlyList<object> events, TimeProvider timeProvider)
        {
            var messages = ToOutboxMessageEntity(events, timeProvider);
            var entries = new List<EntityEntry<OutboxMessage<TKey>>>(messages.Count);
            try
            {
                foreach (var message in messages)
                {
                    entries.Add(Add(message));
                }
            }
            catch
            {
                // Entries added before the failure are not part of any attempt yet, so nothing else would detach them.
                foreach (var entry in entries)
                {
                    entry.State = EntityState.Detached;
                }
                throw;
            }
            return entries;
        }

        private IReadOnlyList<SavedOutboxMessage> CompleteOutboxEntries(
            IReadOnlyList<EntityEntry<OutboxMessage<TKey>>> outboxEntries,
            IReadOnlyList<PayloadPatch> patches,
            bool acceptAllChangesOnSuccess)
        {
            var payloadConverter = GetPayloadConverter();
            var saved = new List<SavedOutboxMessage>(outboxEntries.Count);
            for (var i = 0; i < outboxEntries.Count; i++)
            {
                var entry = outboxEntries[i];
                var message = entry.Entity;
                var originalEvent = message.Payload;
                entry.State = EntityState.Detached;

                message.Id = patches[i].Id;
                if (!acceptAllChangesOnSuccess)
                {
                    // The event instance keeps default keys until the caller accepts, so dispatch what was persisted.
                    message.Payload = payloadConverter.ConvertFromProvider(patches[i].Payload);
                }
                saved.Add(new SavedOutboxMessage(originalEvent, message, acceptAllChangesOnSuccess));
            }
            return saved;
        }

        /// <summary>
        /// Saving with acceptAllChangesOnSuccess = false leaves store-generated values (identity keys, propagated FKs)
        /// in the change tracker only, so the payload serialized by the INSERT carries default keys. The tracker's
        /// current values are projected onto the entity objects while the payloads are re-serialized, then the
        /// previous values are restored; rows whose payload differs get an UPDATE.
        /// The backing field known to EF is used when there is one; only a mapped property without a backing field
        /// is projected through its property setter. When the save ends by accepting the changes
        /// (<paramref name=acceptAllChangesOnSuccess/> = true), AcceptAllChanges writes the same values to those
        /// properties, so their restore is deferred to the failure path and the setter is not called with the
        /// pre-save value on success (as with plain EF); otherwise the setter is invoked again to restore.
        /// </summary>
        private IReadOnlyList<PayloadPatch> BuildPayloadPatches(SaveAttempt attempt, bool acceptAllChangesOnSuccess)
        {
            var businessEntries = attempt.BusinessEntries;
            var outboxEntries = attempt.OutboxEntries;
            var payloadConverter = GetPayloadConverter();
            var idProperty = Model.FindEntityType(typeof(OutboxMessage<TKey>)).FindProperty(nameof(OutboxMessage<TKey>.Id));
            var idConverter = idProperty.GetValueConverter() ?? idProperty.GetTypeMapping().Converter;

            // SaveChanges(false) writes no values to the entity objects, so this is the payload the INSERT wrote.
            var inserted = outboxEntries.Select(x => payloadConverter.ConvertToProvider(x.Entity.Payload)).ToList();

            // When the projection changes no value on any entity object, the inserted payloads are already final and
            // no UPDATE is issued; the payloads are compared only when something was projected.
            var projected = inserted;
            var restore = ProjectTrackedValues(businessEntries);
            var anyProjected = restore.Count > 0;
            if (anyProjected)
            {
                try
                {
                    projected = outboxEntries.Select(x => payloadConverter.ConvertToProvider(x.Entity.Payload)).ToList();
                }
                catch (Exception ex)
                {
                    var restoreErrors = RestoreClrValues(restore);
                    if (restoreErrors.Count > 0)
                    {
                        throw new AggregateException(new[] { ex }.Concat(restoreErrors));
                    }
                    throw;
                }
                if (acceptAllChangesOnSuccess)
                {
                    attempt.DeferredClrRestores.AddRange(restore.Where(x => !(x.Member is System.Reflection.FieldInfo)));
                    restore = restore.Where(x => x.Member is System.Reflection.FieldInfo).ToList();
                }
                ThrowIfAny(RestoreClrValues(restore));
            }

            var patches = new List<PayloadPatch>(outboxEntries.Count);
            for (var i = 0; i < outboxEntries.Count; i++)
            {
                var idEntry = outboxEntries[i].Property(x => x.Id);
                if (idEntry.IsTemporary)
                {
                    throw new InvalidOperationException($"The key of the saved '{typeof(OutboxMessage<TKey>).Name}' is still a temporary value.");
                }
                var id = (TKey)idEntry.CurrentValue;
                var providerId = idConverter != null ? idConverter.ConvertToProvider(id) : id;
                patches.Add(new PayloadPatch(id, providerId, projected[i], anyProjected && !Equals(inserted[i], projected[i])));
            }
            return patches;
        }

        private static List<ClrValue> ProjectTrackedValues(IReadOnlyList<EntityEntry> entries)
        {
            var restore = new List<ClrValue>();
            try
            {
                foreach (var entry in entries)
                {
                    foreach (var property in entry.Properties)
                    {
                        var metadata = property.Metadata;
                        if (metadata.IsShadowProperty() || metadata.IsIndexerProperty() || property.IsTemporary) continue;

                        var member = GetClrMember(metadata);
                        if (member == null) continue;

                        var clrValue = GetClrValue(member, entry.Entity);
                        var currentValue = property.CurrentValue;
                        if (metadata.GetValueComparer().Equals(clrValue, currentValue)) continue;

                        SetClrValue(member, entry.Entity, currentValue);
                        restore.Add(new ClrValue(member, entry.Entity, clrValue));
                    }
                }
            }
            catch (Exception ex)
            {
                var restoreErrors = RestoreClrValues(restore);
                if (restoreErrors.Count > 0)
                {
                    throw new AggregateException(new[] { ex }.Concat(restoreErrors));
                }
                throw;
            }
            return restore;
        }

        /// <summary>Attempts every restore, returning the failures instead of stopping at the first one.</summary>
        private static List<Exception> RestoreClrValues(IReadOnlyList<ClrValue> values)
        {
            var errors = new List<Exception>();
            foreach (var item in values)
            {
                try
                {
                    SetClrValue(item.Member, item.Entity, item.Value);
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }
            }
            return errors;
        }

        /// <summary>
        /// The member used to read and write a mapped property's value on the entity object: the backing field known
        /// to EF when there is one, otherwise the CLR property when it can be read and written (its setter is then
        /// invoked). Returns null for shadow/indexer properties and properties that cannot be written.
        /// </summary>
        private static System.Reflection.MemberInfo GetClrMember(IProperty metadata)
        {
            if (metadata.IsShadowProperty() || metadata.IsIndexerProperty()) return null;
            if (metadata.FieldInfo != null) return metadata.FieldInfo;

            var propertyInfo = metadata.PropertyInfo;
            return propertyInfo != null && propertyInfo.CanRead && propertyInfo.CanWrite ? propertyInfo : null;
        }

        private static object GetClrValue(System.Reflection.MemberInfo member, object entity)
        {
            return member is System.Reflection.FieldInfo field
                ? field.GetValue(entity)
                : ((System.Reflection.PropertyInfo)member).GetValue(entity);
        }

        private static void SetClrValue(System.Reflection.MemberInfo member, object entity, object value)
        {
            if (member is System.Reflection.FieldInfo field)
            {
                field.SetValue(entity, value);
            }
            else
            {
                ((System.Reflection.PropertyInfo)member).SetValue(entity, value);
            }
        }

        private static void ThrowIfAny(List<Exception> errors)
        {
            if (errors.Count == 1) ExceptionDispatchInfo.Capture(errors[0]).Throw();
            if (errors.Count > 1) throw new AggregateException(errors);
        }

        private ValueConverter GetPayloadConverter()
        {
            var payloadProperty = Model.FindEntityType(typeof(OutboxMessage<TKey>)).FindProperty(nameof(OutboxMessage<TKey>.Payload));
            return payloadProperty.GetValueConverter() ?? payloadProperty.GetTypeMapping().Converter
                ?? throw new InvalidOperationException($"'{typeof(OutboxMessage<TKey>).Name}.{nameof(OutboxMessage<TKey>.Payload)}' has no value converter.");
        }

        private string BuildPayloadUpdateSql()
        {
            var entityType = Model.FindEntityType(typeof(OutboxMessage<TKey>));
            var table = StoreObjectIdentifier.Table(entityType.GetTableName(), entityType.GetSchema());
            var payloadColumn = entityType.FindProperty(nameof(OutboxMessage<TKey>.Payload)).GetColumnName(table);
            var idColumn = entityType.FindProperty(nameof(OutboxMessage<TKey>.Id)).GetColumnName(table);
            var sql = this.GetService<ISqlGenerationHelper>();

            // ExecuteSqlRaw formats the text with the parameter placeholders, so braces in identifiers are escaped.
            return $"UPDATE {OutboxSqlText.EscapeFormat(sql.DelimitIdentifier(table.Name, table.Schema))} SET {OutboxSqlText.EscapeFormat(sql.DelimitIdentifier(payloadColumn))} = {{0}} WHERE {OutboxSqlText.EscapeFormat(sql.DelimitIdentifier(idColumn))} = {{1}}";
        }

        /// <summary>
        /// The stored type name (version-less, see <see cref="OutboxTypeName"/>) of every event must fit the column
        /// <c>OutboxMessage.Type</c> is mapped to (200 unless the consumer changed it, see <see cref="EventTypeNameMaxLength"/>;
        /// no check without a limit). Checked on every provider, before anything is sent, so the save fails as a whole with
        /// a clear message instead of a provider error (SQL Server) or a silently longer value (SQLite ignores the length).
        /// </summary>
        private void ThrowIfEventTypeNameTooLong(IReadOnlyList<object> events)
        {
            var property = Model.FindEntityType(typeof(OutboxMessage<TKey>))?.FindProperty(nameof(OutboxMessage<TKey>.Type));
            var maxLength = EventTypeNameMaxLength(property, out var columnType);
            if (maxLength == null) return;

            var checkedTypes = new HashSet<Type>();
            foreach (var @event in events)
            {
                var type = @event.GetType();
                if (!checkedTypes.Add(type)) continue;

                var name = OutboxTypeName.Get(type);
                if (name.Length <= maxLength.Value) continue;

                var keyName = CSharpTypeName(typeof(TKey));
                var limit = columnType == null
                    ? $"OutboxMessage<{keyName}>.Type allows at most {maxLength.Value}"
                    : $"OutboxMessage<{keyName}>.Type allows at most {maxLength.Value} (column type '{columnType}')";
                var raise = columnType == null
                    ? $"modelBuilder.Entity<OutboxMessage<{keyName}>>().Property(x => x.Type).HasMaxLength(n), "
                    : $"modelBuilder.Entity<OutboxMessage<{keyName}>>().Property(x => x.Type).HasColumnType(...) with a larger length, ";
                throw new InvalidOperationException(
                    $"The outbox event type '{name}' cannot be saved: its stored type name is {name.Length} characters long, " +
                    $"but {limit}. Nothing was saved. " +
                    "Use a shorter event type name (namespace, type or generic arguments), or raise the column length in your " +
                    "DbContext's OnModelCreating, after base.OnModelCreating(modelBuilder): " +
                    raise +
                    "and add a migration that changes the column.");
            }
        }

        /// <summary>
        /// The length limit of the column <c>OutboxMessage.Type</c> is mapped to. An explicit column type
        /// (<c>HasColumnType</c>) defines the column, so its length wins: <c>nvarchar(500)</c> or <c>varchar(500)</c> is
        /// 500, <c>nvarchar(max)</c> or a type with no length concept (<c>TEXT</c>, <c>ntext</c>, <c>xml</c>, see
        /// <see cref="UnboundedColumnTypes"/>) has no limit; a column type whose length cannot be read, including a
        /// character type written without a length (<c>nvarchar</c>, <c>varchar</c>, <c>sysname</c>, any unknown name),
        /// falls back to the max length (<paramref name="columnType"/> stays null), like a property without an explicit
        /// column type (<c>HasMaxLength</c>, 200 by default; null: no limit).
        /// </summary>
        private static int? EventTypeNameMaxLength(IProperty property, out string columnType)
        {
            columnType = null;
            if (property == null) return null;

            // The annotation is set only by an explicit column type; the provider's default store type is not an annotation.
            if (property.FindAnnotation(RelationalAnnotationNames.ColumnType)?.Value is string explicitType
                && TryParseColumnTypeLength(explicitType, out var length))
            {
                if (length != null) columnType = explicitType;
                return length;
            }

            return property.GetMaxLength();
        }

        /// <summary>
        /// Store type names (compared case-insensitively, without parentheses) that have no length: SQL Server
        /// <c>text</c>, <c>ntext</c>, <c>xml</c>; SQLite <c>TEXT</c>; PostgreSQL <c>text</c>, <c>citext</c>; MySQL
        /// <c>mediumtext</c>, <c>longtext</c>. Any other type name without parentheses is not assumed to be unlimited
        /// (SQL Server <c>nvarchar</c> alone is <c>nvarchar(1)</c>, <c>sysname</c> is <c>nvarchar(128)</c>).
        /// </summary>
        private static readonly HashSet<string> UnboundedColumnTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "text", "ntext", "xml", "citext", "mediumtext", "longtext",
        };

        /// <summary>
        /// Reads the length of a store type: <c>nvarchar(500)</c> -> 500; <c>nvarchar(max)</c> or a type in
        /// <see cref="UnboundedColumnTypes"/> -> null (no limit). False (the length cannot be read) for any other type
        /// without parentheses (<c>nvarchar</c>, <c>sysname</c>) and when the parentheses hold anything else (for
        /// example <c>decimal(18,2)</c>).
        /// </summary>
        private static bool TryParseColumnTypeLength(string storeType, out int? length)
        {
            length = null;
            var open = storeType.IndexOf('(');
            if (open < 0) return UnboundedColumnTypes.Contains(storeType.Trim());

            var close = storeType.IndexOf(')', open + 1);
            if (close < 0) return false;

            var facet = storeType.Substring(open + 1, close - open - 1).Trim();
            if (string.Equals(facet, "max", StringComparison.OrdinalIgnoreCase)) return true;

            if (int.TryParse(facet, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var parsed) && parsed > 0)
            {
                length = parsed;
                return true;
            }

            return false;
        }

        private static string CSharpTypeName(Type type)
        {
            if (type == typeof(int)) return "int";
            if (type == typeof(long)) return "long";
            if (type == typeof(short)) return "short";
            if (type == typeof(byte)) return "byte";
            if (type == typeof(uint)) return "uint";
            if (type == typeof(ulong)) return "ulong";
            if (type == typeof(ushort)) return "ushort";
            if (type == typeof(sbyte)) return "sbyte";
            return type.Name;
        }

        private ICollection<OutboxMessage<TKey>> ToOutboxMessageEntity(IReadOnlyList<object> events, TimeProvider timeProvider)
        {
            var outboxOptions = this.GetService<IOptions<OutboxConfiguration>>()?.Value;

            return events.Select(x => new OutboxMessage<TKey>
            {
                Type = OutboxTypeName.Get(x.GetType()),
                Payload = x,
                Created = timeProvider.GetUtcNow().DateTime,
                LockExpiresAt = timeProvider.GetUtcNow().DateTime + outboxOptions.MessageLockExpirationTimeSpan
            }).ToList();
        }

        private sealed class SaveAttempt
        {
            public SaveAttempt(
                IReadOnlyList<EntityEntry> businessEntries,
                IReadOnlyList<GeneratedValueSnapshot> generatedValues,
                IReadOnlyList<EntityEntry<OutboxMessage<TKey>>> outboxEntries)
            {
                BusinessEntries = businessEntries;
                GeneratedValues = generatedValues;
                OutboxEntries = outboxEntries;
            }

            public IReadOnlyList<EntityEntry> BusinessEntries { get; }
            public IReadOnlyList<GeneratedValueSnapshot> GeneratedValues { get; }
            public IReadOnlyList<EntityEntry<OutboxMessage<TKey>>> OutboxEntries { get; }

            /// <summary>Setter-only properties left projected because the save accepts the changes; restored only if the attempt fails.</summary>
            public List<ClrValue> DeferredClrRestores { get; } = new List<ClrValue>();

            /// <summary>EF's save of this attempt succeeded: its changes are in the transaction until it ends or is rolled back to a savepoint.</summary>
            public bool BaseSaveCompleted { get; set; }
        }

        private sealed class GeneratedValueSnapshot
        {
            public GeneratedValueSnapshot(PropertyEntry property, object value, bool wasTemporary, System.Reflection.MemberInfo member, object clrValue)
            {
                Property = property;
                Value = value;
                WasTemporary = wasTemporary;
                Member = member;
                ClrValue = clrValue;
            }

            public PropertyEntry Property { get; }
            public object Value { get; }
            public bool WasTemporary { get; }

            /// <summary>The backing field, or the CLR property when the property has no backing field; null when neither is usable.</summary>
            public System.Reflection.MemberInfo Member { get; }
            public object ClrValue { get; }
        }

        private sealed class ClrValue
        {
            public ClrValue(System.Reflection.MemberInfo member, object entity, object value)
            {
                Member = member;
                Entity = entity;
                Value = value;
            }

            /// <summary>The backing field, or the CLR property when the property has no backing field.</summary>
            public System.Reflection.MemberInfo Member { get; }
            public object Entity { get; }
            public object Value { get; }
        }

        private sealed class IdHolder
        {
            public IdHolder(TKey value) => Value = value;

            public TKey Value { get; }
        }

        private sealed class OutboxSaveResult
        {
            public OutboxSaveResult(int result, IReadOnlyList<SavedOutboxMessage> messages)
            {
                Result = result;
                Messages = messages;
            }

            public int Result { get; }
            public IReadOnlyList<SavedOutboxMessage> Messages { get; }
        }

        private sealed class SavedOutboxMessage
        {
            public SavedOutboxMessage(object @event, OutboxMessage<TKey> message, bool acceptedChanges)
            {
                Event = @event;
                Message = message;
                AcceptedChanges = acceptedChanges;
            }

            public object Event { get; }
            public OutboxMessage<TKey> Message { get; }

            /// <summary>
            /// The business changes saved with this message were accepted: by its own save (acceptAllChangesOnSuccess
            /// = true) or by a later save in the same transaction with acceptAllChangesOnSuccess = true.
            /// </summary>
            public bool AcceptedChanges { get; set; }

            /// <summary>
            /// A rollback to a savepoint happened and the check of which rows it undid failed: the row may or may not
            /// exist, so the message is not dispatched in memory until a later check confirms it.
            /// </summary>
            public bool Unconfirmed { get; set; }
        }

        private sealed class PayloadPatch
        {
            public PayloadPatch(TKey id, object providerId, object payload, bool changed)
            {
                Id = id;
                ProviderId = providerId;
                Payload = payload;
                Changed = changed;
            }

            public TKey Id { get; }
            public object ProviderId { get; }
            public object Payload { get; }
            public bool Changed { get; }
        }
    }
}
