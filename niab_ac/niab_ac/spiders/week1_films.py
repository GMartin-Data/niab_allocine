import scrapy


class Week1FilmsSpider(scrapy.Spider):
    name = "week1_films"
    allowed_domains = ["site.com"]
    start_urls = ["https://site.com"]

    def parse(self, response):
        pass
