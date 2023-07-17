import asyncio
import json

import aiofiles
import aioredis


async def main():
    redis = aioredis.from_url("redis://localhost", decode_responses=True)

    async with aiofiles.open("./data/51zyzy_qa_raw/qa.jsonl", "w") as f:
        while True:
            item = await redis.lpop("zyzy_qa:items")
            if item is None:
                # Sleep
                await asyncio.sleep(1)
                continue

            data = json.loads(item)
            await f.write(json.dumps(data, ensure_ascii=False) + "\n")
            print(item["title"])


if __name__ == "__main__":
    asyncio.run(main())
