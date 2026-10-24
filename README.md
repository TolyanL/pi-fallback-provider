# pi-fallback-provider

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Pi extension](https://img.shields.io/badge/pi-extension-blueviolet.svg)](https://pi.dev)

Provider and API-key fallbacks for [Pi](https://pi.dev). When a request is rate-limited before it produces content, the extension retries the same model through the configured extra keys and providers, and offers a manual model choice when no configured provider has the current model.

A fallback never duplicates output: the stream is buffered until the first text or thinking chunk, so a request that fails early can be retried silently, while a failure after content started is passed through as a real error. Nothing is retried unless the error looks like a rate limit.

## Features

- **Extra keys per provider** — retry the same model with each `keys[]` entry before moving on.
- **Provider chain** — walk `providers[]` in order, trying the same model id on each configured provider.
- **Manual escape hatch** — if no provider exposes the active model, Pi asks which model to use instead, with the provider's default listed first.
- **Safe retries** — content is buffered until the first delta, so a failed attempt leaves no half-written answer behind.
- **Rate-limit heuristics** — `rate limit`, `429`, `too many requests`, `overloaded`, `529`, and `quota exceeded` are treated as retryable; every other error is passed through untouched.
- **Quiet feedback** — one warning per failed attempt and one confirmation when a fallback succeeds.

## Requirements

- [Pi](https://pi.dev) with extension support.
- At least one provider logged in or configured in Pi as the fallback target.
- A config file at `~/.pi/agent/fallback.json` (see [Configuration](#configuration)).

No build step and no runtime dependencies: Pi loads the TypeScript entry point directly. `@earendil-works/pi-ai` and `pi-coding-agent` are declared as peer dependencies provided by the host.

## Installation

```sh
pi install git:github.com/TolyanL/pi-fallback-provider
```

Pin a tag or commit:

```sh
pi install git:github.com/TolyanL/pi-fallback-provider@v0.1.0
```

Try it for a single invocation without touching your settings:

```sh
pi -e git:github.com/TolyanL/pi-fallback-provider
```

From a local clone:

```sh
git clone https://github.com/TolyanL/pi-fallback-provider
pi install ./pi-fallback-provider
```

Restart Pi, or run `/reload`, after changing the extension. The package is npm-shaped (`keywords: ["pi-package"]`), so `pi install npm:pi-fallback-provider` works the same way if you publish it.

## Configuration

Create `~/.pi/agent/fallback.json`:

```json
{
  "providers": [
    {
      "name": "provider-id",
      "defaultModel": "model-id",
      "keys": ["optional-extra-api-key"]
    }
  ]
}
```

| Key                        | Required | Meaning                                                                                            |
| -------------------------- | -------- | -------------------------------------------------------------------------------------------------- |
| `providers[].name`         | yes      | Provider id as Pi knows it (the part before `/` in `provider/model`).                              |
| `providers[].defaultModel` | yes      | Model shown first in the manual picker and in `/fallback-status`. It does not route automatically. |
| `providers[].keys`         | no       | Extra API keys tried in order after the default authentication.                                    |

Providers are tried in array order. Pi's default authentication is attempted before the extra `keys`. Keep this file private: it can contain API credentials.

The config is read at `session_start` and then restored from the session, so a change applies after `/reload` or a restart.

## Usage

`/fallback-status` prints the active provider/model and the configured chain:

```text
Current: command-code / claude-sonnet-5
* command-code [2] → claude-sonnet-5
  anymodel [0] → glm/glm-5.3
```

The `*` marks the provider serving the current request.

## How fallback works

For the active model, a request goes through these steps until one succeeds:

1. **Default attempt** — the provider's own `streamSimple`, with the default key.
2. **Extra keys** — the current provider's remaining `keys[]`, one attempt each.
3. **Configured providers** — each `providers[]` entry in order, if it exposes the **same model id**: default auth first, then its extra keys.
4. **Manual selection** — if no configured provider had that model id, a picker lists up to three models per provider (default model first, at most twelve choices plus _Cancel — fail this request_).
5. **Terminal error** — everything exhausted; the last error is reported as the assistant message's `errorMessage`.

Only requests where the error arrives before the first text or thinking chunk are retried. Once content has started, an error is forwarded to Pi unchanged, so a fallback can never leave two partial answers in one message.

Unavailable providers in the chain are skipped with a warning, and their absence does not abort the chain.

## Limitations

- Rate limits are recognised by message text, not by status code — a provider that words them differently will not be retried.
- Automatic fallback requires the same model id on the target provider; there is no routing by `defaultModel`.
- A rate limit that arrives after content started is not retried, by design.
- The config is hand-edited: no command writes `fallback.json`, and `keys` are stored in plain text.
- The chain is evaluated per request for the selected model only; providers that are not configured in `fallback.json` are never tried.

## Development

The extension entry point is `index.ts`; Pi loads TypeScript directly, so there is no build step. Point Pi at a clone and reload after edits:

```sh
pi install ./pi-fallback-provider   # or: pi -e ./pi-fallback-provider
```

Useful entry points in `index.ts`:

- `wrapProviderIfNeeded` — replaces the active provider's `streamSimple` once.
- `createFallbackStream` — intercepts only the active model, so fallback calls cannot recurse.
- `runFallbackChain` — the attempt order described above.
- `proxyStream` — buffering and rate-limit classification.
- `tryManualSelection` — the model picker shown when the chain has nothing to try.

## License

[MIT](LICENSE) © TolyanL
