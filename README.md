# scio_sample

## Raison d'être

TODO: your project description

## Features

This project comes with number of preconfigured features, including:

### Running

The templates ships with beam `DirectRunner` in the `Test` scope. You can run locally with
`sbt Test/runMain example.WordCount --runner=DirectRunner --output=wc`

Run with the selected runner
`sbt runMain example.WordCount --runner=<runner> --output=<path>`
you may have to provide options depending on the runner

### Testing

This template comes with an example of a test, to run tests:

```bash
sbt test
```

### REPL

To experiment with current codebase in [Scio REPL](https://github.com/spotify/scio/wiki/Scio-REPL)
simply:

```bash
sbt repl/run
```

---

This project is based on the [scio.g8](https://github.com/spotify/scio.g8).
