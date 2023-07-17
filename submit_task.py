import asyncio
import json
import os
from pathlib import Path

import aioredis


async def main():
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_password = os.getenv("REDIS_PASSWORD")

    redis = aioredis.from_url(f"redis://:{redis_password}@{redis_host}", decode_responses=True)

    root_path = Path("./data/huatuo_consultation_qa")
    for split in ["train", "validation", "test"]:
        with open(root_path / f"{split}_datasets.jsonl") as f:
            for i, line in enumerate(f):
                sample = json.loads(line)
                for url in sample["answers"]:
                    qa_id = url.split("/")[-1].split(".")[0]
                    await redis.lpush("zyzy_qa:start_urls", json.dumps({
                        "url": url,
                        "meta": {"job-id": qa_id},
                    }))

            if (i + 1) % 100000 == 0:
                print(f"{split} {i + 1} samples processed")

        print("done")


if __name__ == "__main__":
    asyncio.run(main())
