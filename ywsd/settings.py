import copy
import yaml


class Settings:
    _default_map = {
        "TRUSTED_LOCAL_LISTENERS": [],
        "ROUTING_TIME_WARNING_THRESHOLD_MS": 1000,
        "CACHE_CONFIG": {},
    }

    def __init__(self, config_file=None):
        if config_file is None:
            config_file = "routing_engine.yaml"
        with open(config_file, "r") as f:
            self.config = yaml.safe_load(f)

    def __getattr__(self, item):
        return self.config.get(item, copy.copy(self._default_map.get(item)))
