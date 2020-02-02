import yaml

class Settings:
    def __init__(self, config_file=None):
        if config_file is None:
            config_file = "routing_engine.yaml"
        with open(config_file, "r") as f:
            self.config = yaml.load(f, Loader=yaml.CLoader)

    def __getattr__(self, item):
        return self.config.get(item)


