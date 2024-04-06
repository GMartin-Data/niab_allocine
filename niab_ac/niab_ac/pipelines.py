# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from loguru import logger


class BoxOfficePipeline:
    @logger.catch
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        film_id = adapter.get("film_id")
        item["film_id"] = int(film_id)

        entries = adapter.get("entries").replace(" ", "")
        item["entries"] = int(entries)
   
        return item
