from fastapi import FastAPI
from .api.api_v1.api import router as api_router
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Hello welcome to petbarn services"}



app.include_router(api_router, prefix="/api_v1")



#
# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)