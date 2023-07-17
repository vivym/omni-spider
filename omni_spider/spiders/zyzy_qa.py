import re

from scrapy.responsetypes import Response
from scrapy_redis.spiders import RedisSpider


class ZyzyQASpider(RedisSpider):
    name = "zyzy_qa"
    redis_key = "zyzy_qa:start_urls"

    def __init__(self, *args, **kwargs):
        # Dynamically define the allowed domains list.
        domain = kwargs.pop("domain", "")
        self.allowed_domains = filter(None, domain.split(","))
        super().__init__(*args, **kwargs)

    def parse(self, response: Response):
        qa_id = response.url.split("/")[-1].split(".")[0]

        with open(f"./data/51zyzy_qa_raw/html/{qa_id}.html", "w") as f:
            f.write(response.text)

        title = response.css("h1.content-qa-title::text").get()
        question = response.css("div.content-answer-con").get()

        question_time = response.css("div.ask div.content-qa-time span::text").get()

        answers_el = response.css("ul.answer-doc-list li")
        answers = []
        for answer_el in answers_el:
            content = answer_el.css("div.con").get()
            answer_time = answer_el.css("div.content-qa-time span::text").get()
            view_count_str = answer_el.css("a.scan_area em::text").get()
            like_count_str = answer_el.css("a.dianz_area em::text").get()

            view_count_str = re.search(r"\d+", view_count_str).group(0) if view_count_str else None
            view_count = int(view_count_str) if view_count_str else 0
            like_count = int(like_count_str) if like_count_str else 0

            answers.append(
                {
                    "content": content,
                    "answer_time": answer_time,
                    "view_count": view_count,
                    "like_count": like_count,
                }
            )

        yield {
            "url": response.url,
            "qa_id": qa_id,
            "title": title,
            "question": question,
            "question_time": question_time,
            "answers": answers,
        }
