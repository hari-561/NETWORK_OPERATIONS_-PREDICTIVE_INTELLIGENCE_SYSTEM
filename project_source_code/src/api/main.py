from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers.network import router as network_router
from routers.pipeline import router as pipeline_router


app = FastAPI(
    title="Telecom Network Analytics API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(network_router)
app.include_router(pipeline_router)

@app.get("/health")
def health():
    return {
        "status": "ok"
    }