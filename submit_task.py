import asyncio
import json

import aioredis


async def main():
    redis = aioredis.from_url("redis://localhost", decode_responses=True)

    with open("./data/huatuo_consultation_qa/validation_datasets.jsonl") as f:
        for i, line in enumerate(f):
            sample = json.loads(line)
            for url in sample["answers"]:
                qa_id = url.split("/")[-1].split(".")[0]
                await redis.lpush("zyzy_qa:start_urls", json.dumps({
                    "url": url,
                    "meta": {"job-id": qa_id},
                }))
            if i == 3000:
                break


if __name__ == "__main__":
    asyncio.run(main())
