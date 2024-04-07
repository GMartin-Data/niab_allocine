import scrapy


class TrainFilmsSpider(scrapy.Spider):
    name = "train_films"
    allowed_domains = ["site.com"]
    start_urls = ["https://site.com"]

    def parse(self, response):
        pass
