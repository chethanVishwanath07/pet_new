from pydantic import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    AWS_KEY = "AKIA4EQK62742QAW2273"
    AWS_SECRET = "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"
    AWS_REGION = "ap-south-1"
    AWS_BUCKET_NAME = "provenio-petbarn"
    DB_USERNAME =  "postgres"
    DB_PASSWORD = "PetbarN123"
    DB_HOST = "petbarn.cssm0yfvyzaa.ap-southeast-2.rds.amazonaws.com"
    DB_PORT = "5432"
    DB_NAME = "petbarn"


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
