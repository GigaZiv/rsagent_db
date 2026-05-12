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
        for idx in range(1, 9):  # Отправим 5 задач
            task_id = str(uuid.uuid4())
            task_data = {
                "id": task_id,
                "type": "send",
                "check": True,
                "lindb": True,
                "payload": {
                    "phones": f"7912835598{idx}",
                    "message": f"Test message #{idx} for ID {task_id[:8]}"
                }
            }

            # Пишем в очередь
            await redis_engine_client.rpush('task_queue_sms', json.dumps(task_data))
            print(f"Task {idx} added: {task_id}")

    except Exception as e:
        print(f"Test error: {e}")
    finally:
        await redis_engine_client.aclose()
        print("--- Seeding complete ---")

if __name__ == '__main__':
    asyncio.run(over_test())
