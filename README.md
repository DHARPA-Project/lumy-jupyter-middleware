# Lumy middleware

Server side part of [Lumy](https://github.com/DHARPA-Project/lumy).

## Installation

### Development

Lumy middleware depends on a number of [Kiara](https://github.com/DHARPA-Project/kiara) which has its releases hosted on [Gemfury](https://fury.io).
The right way to install this package in development mode is:

```shell
pip install -U --extra-index-url https://pypi.fury.io/dharpa/ -e .
```

## API

The middleware employs a pub sub pattern for communicating with the front end. There are [several channels](lumy_middleware/target.py) set up by the middleware that the front end can subscribe to in order to receive messages. The front end can also post messages on channels. Each channel supports a set of messages. All messages are defined as JSON schemas that can be found [here](https://github.com/DHARPA-Project/lumy/tree/master/schema/json). Every time messages are updated, message classes need to be generated for both the front end code and the middleware. Generated classes for the middleware are located in [this file](lumy_middleware/types/generated.py).

### Generating message classes

Message classes are generated from JSON schema files using a [generator tool](https://github.com/DHARPA-Project/lumy/tree/master/tools) from the Lumy front end package. The code generation process is started from the `tools` directory:

```shell
yarn gen  -- --middleware-directory=../../lumy-middleware
```

### Workflow modules

#### Execution side

Execution of workflows is handled by Kiara. Kiara workflow modules are installed as Python packages and modules are discovered during run time. See [Kiara documentation](https://github.com/DHARPA-Project/kiara) to understand how to make modules discoverable.

#### User Interface side

User interface modules are written in JavaScript. They are declared in the Lumy workflow files (under `ui/pages/component/url`) and can be discovered using several methods:

 * HTTP/HTTPS link to the workflow JavaScript file.
 * A path to the JavaScript file on disk.
 * A name of a Lumy module plug-in provided by a Python package, which returns the content of the JavaScript file containing UI modules code.

##### Link

A link in the form of `https://domain.com/file.js`.

##### Path to the file

A path in the form of `file://../path/to/the/file.js`

##### Lumy module plug-in

A URI in the form of `lumymodule://my-plug-in`.

To enable Lumy plug-ins in a Python package, the following code should be added to the `entry_points` in the `setup.py` or the `setup.cfg` files:

```python
'lumy.modules': [
    'plug_in_id = lumy_modules.my_plug_in:get_code'
]
```

Where:

 * `plug_in_id` is the name of the plug-in.
 * `get_code` is the name of a function which returns the content of the JavaScript file containing plug-in code. The actual file is usually stored in the resources section of the package.
 * `lumy_modules.my_plug_in` is the package containing the `get_code` function (in this example it could be in the `__init__.py` file)

This plug-in later can be referenced in a workflow as `lumymodule://plug_in_id`.

This mechanism is similar to the one used in Kiara.

## Releases

A new release is published to [Gemfury](https://fury.io) every time the repository is tagged with a `v*` tag, where `*` after `v` is a semver version, that should match the version declared in `lumy_middleware.__init__.py` [file](lumy_middleware/__init__.py) and it's not checked whether the tag matches the version declared in the file.


