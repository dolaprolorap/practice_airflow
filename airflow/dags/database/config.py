import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    stocks_db_user: str
    stocks_db_password: str
    stocks_db_host: str
    stocks_db_name: str

    model_config = SettingsConfigDict(env_file=os.path.join(os.path.dirname(__file__), "../.env"), extra='allow')

