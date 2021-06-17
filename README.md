# algo-trader

Crypto momentum trading

## What is this project?

The goal of this repo is to become a fully automated trading application. See below for progress and todos.

Endpoints for both ftx.com and ftx.us are included, use the ones for your jurisdiction. This is an opinionated approach and is definitely not the only way to trade.

## Features/TODO:

 * ~~Async plumbing~~
 * ~~Market data subscription~~
 * ~~Initial datadog/statsd metrics~~
 * ~~Elementary bar formation~~ (only implemented what I needed for the time being, will consider it done)
 * ~~Rough momentum algo~~
 * ~~Forecast scaling~~
 * ~~Slippage/costs estimation~~
 * ~~Position management~~
 * ~~Bet sizing~~
 * ~~Backtesting~~
 * ~~Paper tradering~~
 * ~~Portfolio optimization~~ (now out of scope for my model)
 * Order placing logic
 * Order management
 * Equity initialization
 * Runtime support for:
   * trading
   * paper trading
   * ~~backtesting~~
   * persisting live data
   * ~~querying historical data~~
 * ~~Make schema multi-symbol~~

## Requirements
 * Java 8 or higher
 * leiningen
 * FTX API key for your own account
 * Datadog account (optional) and [datadog agent](https://docs.datadoghq.com/agent/)

## Implementation/discussion

Clojure was chosen partially for personal preference and also because of the two libraries below; also, aren't there enough python traders out there?

aleph was chosen because I love netty and the event loop for throughput. Even though aleph hasn't received as much regular support lately, I still think it's the best choice for websockets.

core.async was chosen for similar reasons - we want to spawn tons of go-loops to handle each pair's quotes and trades.

## Usage

Clone or fork the repo and make changes as needed.

Absolutely no guarantees are made about this repo or its functionality, use at your own discretion. This repo does not represent financial advice. Crypto is a risky asset, please do your own research.

## License

Copyright © 2021 Daniel Lee

Distributed under the MIT License.
