import asyncio
import json
import pathlib
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

import database
import worker as worker_module

UPLOADS_DIR = pathlib.Path("uploads")
task_queue: asyncio.Queue = asyncio.Queue()


@asynccontextmanager
async def lifespan(app: FastAPI):
    UPLOADS_DIR.mkdir(exist_ok=True)
    database.init_db()

    for task in database.get_stuck_tasks():
        database.set_failed(task["id"], "Server restarted during processing")

    for task in database.get_pending_tasks():
        await task_queue.put(task["id"])

    worker_task = asyncio.create_task(worker_module.run_worker(task_queue))
    yield
    worker_task.cancel()


app = FastAPI(lifespan=lifespan)


@app.post("/datasets")
async def upload_dataset(file: UploadFile):
    if not file.filename.endswith(".json") and file.content_type not in (
        "application/json",
        "text/json",
    ):
        raise HTTPException(status_code=400, detail="File must be a .json file")

    raw = await file.read()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="File is not valid JSON")

    if "dataset_id" not in data:
        raise HTTPException(status_code=400, detail="Missing required field: dataset_id")

    task_id = str(uuid.uuid4())
    (UPLOADS_DIR / f"{task_id}.json").write_bytes(raw)
    database.create_task(task_id, data["dataset_id"], file.filename)
    await task_queue.put(task_id)

    return JSONResponse({"task_id": task_id, "status": "pending"})


@app.get("/tasks")
def list_tasks():
    return database.get_all_tasks()


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    task = database.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@app.post("/tasks/{task_id}/retry")
async def retry_task(task_id: str):
    if not database.reset_task(task_id):
        raise HTTPException(status_code=400, detail="Task not found or not in failed state")
    await task_queue.put(task_id)
    return {"task_id": task_id, "status": "pending"}


@app.delete("/tasks/{task_id}")
def delete_task(task_id: str):
    if not database.delete_task(task_id):
        raise HTTPException(status_code=404, detail="Task not found")
    return {"deleted": task_id}


app.mount("/", StaticFiles(directory="static", html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
