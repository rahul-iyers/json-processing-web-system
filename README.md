## How to run
```bash
pip install -r requirements.txt
python main.py
# open http://localhost:8000
```

`uvicorn main:app --reload` also works for development.

## Design choices
**Single-process async architecture.** The FastAPI app and the background worker share the same event loop. An `asyncio.Queue` passes task IDs (not file bytes) between the route handler and the worker coroutine, which is started with `asyncio.create_task()` inside the `lifespan` context manager. This avoids any external dependencies while keeping the upload handler non-blocking.

**Write-before-enqueue.** The task row is written to SQLite as `pending` before the ID is placed on the queue. On startup, any tasks left in `processing` (from a crash mid-job) are immediately failed, and `pending` tasks are re-enqueued which ensures that no work is silently lost.

**`await asyncio.sleep(15)`** is used inside the worker to simulate the long computation, keeping the event loop free to serve HTTP requests during that window.

**Uploads stored as files.** Raw JSON bytes are saved to `uploads/{task_id}.json`. Only the string task ID travels over the queue, keeping memory usage flat regardless of file size.
