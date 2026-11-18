# Azure Service Bus (XDev.Outbox.ServiceBus)

## Status

This component is a prototype. It is not usable as an outbox transport in its
current state:

- `ServiceBusMessageQueue<TKey>` exposes the same members as
  `IMessageQueue<TKey>`, but it implements only `IAsyncDisposable`, not
  `IMessageQueue<TKey>`. It therefore cannot be registered as the queue.
- There is no DI registration extension, the sample does not reference it,
  and the project has no package metadata, unlike the other two libraries.
- Intent is unknown. It could be planned work or an abandoned experiment.

## Observed Behavior

- **Configuration**: `ServiceBusMessageQueueOptions` provides `QueueName`, an
  optional `SerializerOptions` (default is camelCase and indented), and an
  optional `ServiceBusProcessorOptions` (default
  `AutoCompleteMessages = false`). The `ServiceBusClient` is injected; this
  component handles no credentials.
- **Send**: The whole `OutboxMessage<TKey>` is serialized to JSON and sent as
  UTF-8 bytes. Transient `ServiceBusException`s are retried after 1 s, 4 s,
  10 s, 20 s, and 30 s, then rethrown.
- **Receive**: If no handler is attached, the message is abandoned.
  Otherwise the message is deserialized and the handler is invoked. On
  success the message is completed; on any exception it is abandoned, so
  Service Bus redelivery and dead-lettering apply.
- If it were wired in, each failed message would be retried by both Service
  Bus abandon/redelivery and the outbox store's `Attemps` bookkeeping. How
  these two retry mechanisms should interact has not been designed.
- `DisposeAsync` disposes the processor but not `_sender`.
