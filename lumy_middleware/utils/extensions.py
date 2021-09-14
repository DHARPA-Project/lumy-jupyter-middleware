from typing import Any, Optional

from kiara import Kiara
from kiara.module_mgmt.merged import MergedModuleManager
from stevedore import ExtensionManager
from stevedore import _cache as stevedore_cache


def get_plugin(
    namespace: str,
    plugin_name: str
) -> Optional[Any]:
    mgr = ExtensionManager(
        namespace="lumy.modules",
        invoke_on_load=False,
        propagate_map_exceptions=True
    )

    for module in mgr:
        if module.name == plugin_name:
            return module.plugin()
    return None


def reset_cache() -> None:
    '''
    Using internal properties of `stevedore`. This is based on studying
    the code from here: https://github.com/openstack/stevedore/blob/3.3.0/stevedore/_cache.py#L147-L175 # noqa

    NOTE: Please review this if stevedore is upgraded from version 3.3.0 as
    this API may change.
    '''
    stevedore_cache._c._internal = {}
    ExtensionManager.ENTRY_POINT_CACHE = {}


def reset_kiara_cache(kiara_instance: Kiara) -> None:
    '''
    NOTE: Kiara does not provide an interface for refreshing
    internal cache of modules and Kiara developers are not
    planning to add this feature.

    We are using internal API of Kiara which may change
    in new versions of Kiara. Please review this
    when Kiara version is changed.
    '''
    kiara_instance._module_mgr = MergedModuleManager(
        Kiara.instance()._config.module_managers
    )
