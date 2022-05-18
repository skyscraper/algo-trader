# algo-trader

Crypto momentum trading

## What is this project?

This is a fully automated (and semi-opinionated) trading application.

## High level description of functionality

Roughly speaking, I subscribe to futures market data via websocket from all major CEXs. I then form fixed-volume width bars and attempt to generate signals based on the momentum (moving average crossovers and some light TA) of a given token. The signals are meant to be continuous and reflect a desired position in the market, which the OMS handles in the spot market on [FTX.us](https://ftx.us).

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
* ~~Paper trading~~
* ~~Portfolio optimization~~ (now out of scope for my model)
* ~~Order placing logic~~
* ~~Order management~~
* Equity initialization
* Runtime support for:
    * ~~trading~~
    * ~~paper trading~~
    * ~~backtesting~~
    * persisting live data
    * ~~querying historical data~~
* ~~Make schema multi-symbol~~

## Requirements
* Java 8 or higher
* leiningen
* FTX API key for your own account
* Datadog account (optional) and [datadog agent](https://docs.datadoghq.com/agent/)

## Technical implementation

Clojure was chosen partially for personal preference and also because of the two libraries below; also, aren't there enough python traders out there?

aleph was chosen because I like netty and the event loop for throughput. Even though aleph hasn't received as much regular support lately, I still think it's the best choice for websockets.

core.async was chosen for similar reasons - we want to spawn tons of go-loops to handle each token's quotes and trades.

## So, does it make any money?

The short answer is no.

But I do think there is potential here. I am very satisfied with the overall architecture of the market data subscription and aggregation, along with the OMS capabilities... with the exception of the market orders, lol (this was never the long-term intention, it's only being used to prove the general concept). The signal-generation model is probably where the most work needs to be done. Additionally, given the current trading costs (both liquidity-taker fees and slippage), I think this system currently places trades far too frequently and needs to be dialed in correctly.

### Open discussion questions

* Which is the best AWS datacenter for a deploy given that the websocket data is mostly coming from APAC but our trading venue is in the US?
* Given that we're trying to trend follow, I opted to subscribe to perpetual futures data because there is much more volume than in the spot markets (and the price seems to lead spot markets as well). However, given that I'm located in the US and can only trade spot, should I actually be subscribed to spot data? My assumption is also that this delta matters less as the time horizon becomes less granular, but I also can't say for certain.

## Usage

Clone or fork the repo and make changes as needed.

Absolutely no guarantees are made about this repo or its functionality, use at your own discretion. This repo does not represent financial advice. Crypto is a risky asset, please do your own research.

## License

Copyright © 2022 Daniel Lee

Distributed under the MIT License.
