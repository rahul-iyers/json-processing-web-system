import asyncio
import json
import pathlib
import database
import processing

UPLOADS_DIR = pathlib.Path("uploads")


async def run_worker(queue: asyncio.Queue) -> None:
    """runs forever. pull task_id from queue, process it, update db."""
    while True:
        task_id = await queue.get()
        try:
            database.set_processing(task_id)
            file_path = UPLOADS_DIR / f"{task_id}.json"
            data = json.loads(file_path.read_text())
            await asyncio.sleep(15)
            result = processing.process_dataset(data)
            database.set_completed(task_id, result)
        except Exception as e:
            database.set_failed(task_id, str(e))
        finally:
            queue.task_done()
