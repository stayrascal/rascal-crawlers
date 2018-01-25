from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from crawlers.spiders import QuotesSpider

if __name__ == '__main__':
    settings = get_project_settings()
    process = CrawlerProcess(settings)

    process.crawl(QuotesSpider)

    process.start()