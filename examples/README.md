# Matrix SDK examples

In this folder you find examples for using the matrix-sdk (and its various
components) to implement specific common features, each as a separate crate. You
can run each of them from the root of the repository, mind you that `_` becomes
`-` and the crate names are prefixed with `example-`. So to run the image bot
example you can do `cargo run -p example-image-bot`.

The event stream AI bot uses MSC4471 transient updates while it receives text
from OpenAI's Responses API. Set `OPENAI_API_KEY`, mention the logged-in bot in
a room message, and run it with `--auto-join` if the bot should automatically
accept room invites:

```sh
cargo run -p example-event-stream-ai-bot -- <homeserver_url> <username> <password> --auto-join
```

To initialize the bot's Matrix recovery and room key backup, set a recovery
passphrase. On first login this creates the backup; on later logins the bot
uses the passphrase to recover the existing backup:

```sh
MATRIX_RECOVERY_PASSPHRASE=<recovery_passphrase> OPENAI_API_KEY=<openai_api_key> \
    cargo run -p example-event-stream-ai-bot -- <homeserver_url> <username> <password> --auto-join
```
