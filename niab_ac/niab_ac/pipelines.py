# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from loguru import logger

from .utils import convert_dates, convert_duration


LIST_FIELDS = ("casting", "director", "genres", "nationality")


class BoxOfficePipeline:
    @logger.catch
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        film_id = adapter.get("film_id")
        item["film_id"] = int(film_id)

        entries = adapter.get("entries").replace(" ", "")
        item["entries"] = int(entries)
   
        return item


class CleanPipeline:
    @logger.catch
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        for field in LIST_FIELDS:
            # Join with '|' as a separator
            value = adapter.get(field)
            try:
                value = [item.strip() for item in value]
                adapter[field] = "|".join(value)
            except BaseException:
                adapter[field] = None

        # Release (convert it to format 'YYYY-MM-DD')
        release = adapter.get("release")
        if release is not None:
            adapter["release"] = convert_dates(release)
        else:
            adapter["release"] = None

        # Duration (convert to minutes)
        duration = adapter.get("duration")
        if duration is not None:
            adapter["duration"] = convert_duration(duration)
        else:
            adapter["duration"] = None
        
        # Budget (remove trailing spaces, but keep currency symbol)
        budget = adapter.get("budget")
        if budget is not None:
            if budget == '-':
                adapter["budget"] = None
            else:
                adapter["budget"] = budget.replace(" ", "")
        else:
            adapter["budget"] = None

        # Societies (remove duplicates, then join)
        societies = adapter.get("societies")
        try:
            societies = set(item.strip() for item in societies)
            adapter["societies"] = "|".join(societies)
        except BaseException:
            adapter["societies"] = None
   
        return item
