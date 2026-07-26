---
'@mastra/client-js': patch
---

Fixed corrupted characters in streamed responses. Each network chunk was decoded with its own `TextDecoder`, so any non-ASCII character whose bytes happened to land on a chunk boundary reached your code as `�`. The decoder is now shared across the whole stream, which covers `agent.stream()`, workflow `run.stream()` and `run.observe()`, the agent-builder streams, and `streamBackgroundTasks()`.
