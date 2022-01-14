This is a demo project to play around with `&str`, `String` and the borrow checker.

The binary reads a file where each line is an arbitrary json object that includes a field called `type`. It outputs a table where each row
contains the number and total byte size of all the messages for a given `type`, as follows:

| Type          | Count  | Size Bytes |
|---------------|--------|------------|
|         nulla | 302400 |  272924962 |
|        dolore | 121260 |   96825292 |
|          sint |  60630 |   51655909 |


The results of parsing a 5GB file are:

- Lines: 6,048,750
- Unique `types`: 47
- Time: 6.8 seconds
- Throughput: 750 MB/s

## How to try it

To run the tests:

```shell
$ cargo test
```

To run the server, build the binary in release mode:

```shell
$ cargo build --release
```

and execute it:

```shell
$ ./target/release/word-counter
```

## Config

To change the parameters used by the binary, copy the `config.toml.sample` file as `config.toml` and modify it as desired.
