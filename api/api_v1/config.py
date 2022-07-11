from pydantic import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    AWS_KEY = "p"
    AWS_SECRET = "p"
    AWS_REGION = "p"
    AWS_BUCKET_NAME = "p"
    DB_USERNAME =  "p"
    DB_PASSWORD = "P"
    DB_HOST = "p"
    DB_PORT = ""
    DB_NAME = "p"


    # AWS_KEY :str
    # AWS_SECRET :str # = 'bByDQGVi5t93S/f3U1M1edzhmev0FwZkUuDqxboR'
    # AWS_REGION :str #= "ap-south-1"
    # AWS_BUCKET_NAME :str #= "provenio-petbarn"
    # DB_USERNAME: str
    # DB_PASSWORD: str
    # DB_HOST: str
    # DB_PORT: str
    # DB_NAME: str
    #
    # class Config:
    #     env_file = "api_v1/.env"

@lru_cache()
def get_settings():
    return Settings()
