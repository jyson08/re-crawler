from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class CrawlResult:
    source: str
    payload: dict[str, Any]


class BaseCrawler(ABC):
    @abstractmethod
    def crawl(self) -> list[CrawlResult]:
        raise NotImplementedError


class ExampleCrawler(BaseCrawler):
    def crawl(self) -> list[CrawlResult]:
        return [
            CrawlResult(
                source="example",
                payload={"message": "crawler initialized"},
            )
        ]
