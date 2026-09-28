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
            task_id = str(uuid.uuid4())
            task_data = {
                "payload": {
                    "id": "88518aa7-a6a8-44eb-a356-fde3bf03887e",
                    "user_id": 2,
                    "action": "convert_type_01",
                    "file_path": f"test/report1.xml"
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
