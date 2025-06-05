from fastapi import FastAPI, JSONResponse

app = FastAPI()

@app.get("/")
def root():
    return JSONResponse(
        content={"message": "server healthy"},
        status_code=200
    )
