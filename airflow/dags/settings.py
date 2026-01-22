import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    skip_train: bool
    visualisation_path: str

    model_config = SettingsConfigDict(env_file=os.path.join(os.path.dirname(__file__), "./.env"), extra='allow')
