import yaml


class Settings:
    def __init__(self, config_file=None):
        if config_file is None:
            config_file = "routing_engine.yaml"
        with open(config_file, "r") as f:
            self.config = yaml.safe_load(f)

    def __getattr__(self, item):
        return self.config.get(item)
