import scrapy

class YouXinUsedSpider(scrapy.spider):
    name = "youxin"

    def start_requests(self):
        start_urls = [
            'https://www.xin.com/quanguo/',
            'http://www.auto18.com/',
            'https://www.renrenche.com/']

        tag = getattr(self, 'tag', 'quanguo')



