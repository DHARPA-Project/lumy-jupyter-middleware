# Lumy middleware

Server side part of [Lumy](https://github.com/DHARPA-Project/lumy).

## Installation

### Development

Lumy middleware depends on [Kiara](https://github.com/DHARPA-Project/kiara) which has its releases hosted on [Gemfury](https://fury.io).
The right way to install this package in development mode is:

```shell
pip install -U --extra-index-url https://pypi.fury.io/dharpa/ -e .
```

## API

The middleware employs a pubsub pattern for communicating with the front end. There are [several channels](lumy_middleware/target.py) set up by the middleware that the front end can subscribe to in order to receive messages. The front end can also post messages on channels. Each channel supports a set of messages. All messages are defined as JSON schemas that can be found [here](https://github.com/DHARPA-Project/lumy/tree/master/schema/json). Every time messages are updated message classes need to be generated for both the front end and the middleware. Generated classes for the middleware are located in [this file](lumy_middleware/types/generated.py).

#### Generating message classes

Message classes are generated from JSON schema files using a [tool](https://github.com/DHARPA-Project/lumy/tree/master/tools) from the Lumy front end package. The code generation process is started from the `tools` directory:

```shell
yarn gen  -- --middleware-directory=../../lumy-middleware
```

## Releases

A new release is published to [Gemfury](https://fury.io) every time the repository is tagged with a `v*` tag, where `*` after `v` is a semver version, that should match the version declared in `lumy_middleware.__init__.py` file.
