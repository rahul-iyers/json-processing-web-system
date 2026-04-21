A web application for uploading JSON datasets, processing them asynchronously, and viewing results — built with FastAPI and a browser-based UI.

## Requirements
- Python 3.10+
- Dependencies: `fastapi`, `uvicorn[standard]`, `python-multipart` (all in `requirements.txt`)

## How to run
```bash
pip install -r requirements.txt
python main.py
open http://localhost:8000
```

## REST API
`POST` | `/datasets` | Upload a JSON file and queue it for processing |
`GET` | `/tasks` | List all tasks |
`GET` | `/tasks/{task_id}` | Get a single task with full results |
`POST` | `/tasks/{task_id}/retry` | Re-enqueue a failed task |
`DELETE` | `/tasks/{task_id}` | Delete a task |

## Backend design choices
**Single-process async architecture.** The FastAPI app and the background worker share the same event loop. An `asyncio.Queue` passes task IDs between the route handler and the worker coroutine, which is started with `asyncio.create_task()` inside the `lifespan` context manager. This avoids any external dependencies while keeping the upload handler non-blocking.

**Write-before-enqueue.** The task row is written to SQLite as `pending` before the ID is placed on the queue. On startup, any tasks left in `processing` (from a crash mid-job) are immediately failed, and `pending` tasks are re-enqueued which ensures that no work is silently lost.

**`await asyncio.sleep(15)`** is used inside the worker to simulate long computation, keeping the event loop free to serve HTTP requests during that window.

**Uploads stored as files.** Raw JSON bytes are saved to `uploads/{task_id}.json`.
