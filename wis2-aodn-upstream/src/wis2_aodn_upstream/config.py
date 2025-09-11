import os
import yaml

def load_config(config_name: str) -> dict:
    """
    Loads a YAML configuration file from the config directory.

    Args:
        config_name: The name of the configuration file to load (e.g., 'APOLLO_BAY.yaml').

    Returns:
        A dictionary containing the configuration data.
    """
    config_path = os.path.join(os.path.dirname(__file__), 'config', config_name)
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
