# algo-trader

Boilerplate repo for algorithmic trading.

## What is this project?

This app is some boilerplate code that I use in my personal algo trader for both US equities and crypto trading. I generally like the pattern that I used for subscribing and handling events so I'm putting it up here in case anyone else finds it useful.

At the moment, it connects to a market data feed via websocket ([polygon.io](https://polygon.io/) in this case) and subscribes to some predefined symbols for both quotes and trades.

## What this project is NOT

This project is a good starting point, but is definitely not a complete solution. Some notable gaps that will need to be implemented:

* Actual Trade/Quote handling logic
* Broker connection and OMS functionality
* Modeling/forecasting

## Seems like a lot of holes... what does this project do well?

We can subscribe to and process market data pretty quickly (relatively speaking). On average, we receive and parse a trade about 15ms after the SIP timestamp, with the 99th percentile at 49ms. For quotes, the the average is 16ms and 99th percentile is 65ms.

These numbers were achieved on a modestly sized GCP instance (e2-highcpu-8) with basically no tuning of anything except for bumping the max heap up a bit for the JVM. CPU usage hovered around 5% so there was plenty of horsepower left over for prediction logic. Depending on what strategy you want to run, this could be a pretty decent starting point. Plus, the whole thing is less than 200 lines of clojure code and the uberjar is about 17MB, so we're doing pretty well as a lightweight starting point.

## Implementation/discussion

aleph was chosen because I love netty and the event loop for throughput. Even though aleph hasn't received as much regular support lately, I still think it's the best choice for websockets.

core.async was chosen for similar reasons - we want to spawn tons of go-loops to handle each symbol's quotes and trades.

While 15ms is quite good, it's all relative. You should absolutely not use this framework for any strategy that is predicated on having HFT-level speed. If you are using this code, you are probably not collocated in NY4. And if you ARE collocated in NY4, you should absolutely not use this code, you will most likely need an architecture that is single threaded with no garbage collection - but that's another project for another day.

## Usage

clone or fork the repo and make changes as needed.

## License

Copyright © 2021 Daniel Lee

Distributed under the MIT License.
