import asyncio
import json

import aiofiles
import aioredis


async def main():
    redis = aioredis.from_url("redis://localhost", decode_responses=True)

    processed = 0
    async with aiofiles.open("./data/51zyzy_qa_raw/qa.jsonl", "a") as f:
        while True:
            item = await redis.lpop("zyzy_qa:items")
            if item is None:
                # Sleep
                await asyncio.sleep(1)
                continue

            data = json.loads(item)
            await f.write(json.dumps(data, ensure_ascii=False) + "\n")
            processed += 1
            print(processed, data["title"])


if __name__ == "__main__":
    asyncio.run(main())
