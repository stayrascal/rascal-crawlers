import scrapy

class QuotesSpider(scrapy.Spider):
    name = "quotes"

    def start_requests(self):
        urls = [
            'http://quotes.toscrape.com/page/1/',
            'http://quotes.toscrape.com/page/2/']
        
        tag = getattr(self, 'tag', None)

        for url in urls:
            if tag is not None:
                url = url + 'tag/' + tag
            yield scrapy.Request(url=url, callable=self.parse)

    def parse(self, response):
        for quote in response.css('div.quote'):
            yield {
                'text': quote.css('span.text::text').extract_first(),
                'author': quote.css('small.author::text').extract_first(),
                'tags': quote.css('div.tags a.tag::text').extract(),
            }
        
        for a in response.css('li.next a'):
            yield response.follow(a, callback=self.parse)