# `miniredis`

`miniredis` is a pure Python server that supports a subset of the redis protocol.

## Why?

The original intent was to have a minimally working (if naïve) PubSub implementation in order to get to grips with the [protocol spec](http://redis.io/topics/protocol), but I eventually realised that a more complete server would be useful for testing and inclusion in some of my projects.

## Performance

Extremely dependent on Python runtime and workload, of course. Your mileage may vary.

## Credits

I started out by forking [coderanger/miniredis](https://github.com/coderanger/miniredis) for experimentation, and things kind of accreted from there as I started implementing more commands.