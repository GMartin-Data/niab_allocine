import scrapy


class BoxOfficeSpider(scrapy.Spider):
    name = "box_office"
    allowed_domains = ["site.com"]
    start_urls = ["https://site.com"]

    def parse(self, response):
        pass
