import asyncio
import json
import os
import uuid

import redis.asyncio as redis

from utils.rs_env import rs_settings

rs_settings.load_env_file()

redis_engine_client = redis.Redis(
    host=os.getenv("REDIS_HOST", 'localhost'),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    db=int(os.getenv("REDIS_DB_PUSH", 2)),
    decode_responses=True
)


async def over_test():
    print("--- Start seeding test tasks ---")
    try:
        for idx in range(1, 2):
            task_id = uuid.uuid4().hex
            task_data = {
                "payload": {
                    "id": task_id,
                    "action": "convert_type_01",
                    "task_prc": "rs.load_cpt_01_directory",
                    "link_user_id": 2,
                    "task_file": {
                        "task_file_path": "test/report2.xml"
                    }
                }
            }

            await redis_engine_client.rpush(f'rs:agent:taskdb', json.dumps(task_data))
            print(f"Task {idx} added: {task_id}")

    except Exception as e:
        print(f"Test error: {e}")
    finally:
        await redis_engine_client.aclose()
        print("--- Seeding complete ---")

if __name__ == '__main__':
    asyncio.run(over_test())
