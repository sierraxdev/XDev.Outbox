# Integrations

Document external systems and integration behavior that future changes must
understand.

Examples:

- `service-bus.md`
- `sales-api.md`
- `payment-provider.md`
- `file-import.md`

Useful topics include:

- purpose;
- direction of communication;
- message or request contract;
- expected responses;
- retries;
- timeout behavior;
- duplicate delivery behavior;
- idempotency;
- failure handling;
- authentication approach without secrets;
- ordering requirements;
- transaction implications;
- local persistence effects.

Do not store credentials, access tokens, or secrets.
