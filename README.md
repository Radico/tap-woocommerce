# tap-woocommerce

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/README.md).

This tap:

- Pulls raw data from [FIXME](http://example.com)
- Extracts the following resources:
  - [FIXME](http://example.com)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

Setup:

- Run `python3 setup.py install` after cloning
  - NOTE: You will need to re-run this script after making any changes to the
    code in order to compile your changes
- Create a `config.json` file, using `sample_config.json` as a model
- Create a `state.json` file
  - This will likely look like the following:

```json
{
  "bookmarks": {}
}
```

Running the tap:
`tap-woocommerce --config=config.json --state=state.json --properties=catalog.json`

---

Copyright &copy; 2018 Stitch
