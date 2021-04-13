# algo-trader

crypto momentum trading - no guarantees 

## What is this project?

~~This app is some boilerplate code that I use in my personal algo trader for both US equities and crypto trading. I generally like the pattern that I used for subscribing and handling events so I'm putting it up here in case anyone else finds it useful.~~

I am changing the focus of this repo. The boilerplate still applies to anyone wanting to trade equities, but I am going to build this out into a fully automated momentum trader with ftx (endpoints for both ftx.com and ftx.us are included, depending on your jurisdiction). This is an opinionated approach and anyone who forks this is free to change the algo.

## Features/TODO:

 * ~~Async plumbing~~
 * ~~Market data subscription~~
 * ~~Initial datadog/statsd metrics~~
 * ~~Elementary bar formation~~ (only implemented what I needed for the time being, will consider it done)
 * ~~Rough momentum algo~~
 * ~~Forecast scaling~~
 * Slippage/costs estimation
 * Order placing logic
 * Order management
 * Position management
 * Backtesting (this might end up being a separate repo since I need to fork off of what I have privately)
 * Portfolio optimization

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
