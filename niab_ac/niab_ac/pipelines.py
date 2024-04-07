# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from typing import Optional

from itemadapter import ItemAdapter
from loguru import logger


LIST_FIELDS = ("casting", "director", "genres", "nationality")


def convert_dates(date: str) -> str:
    """Convert Allocine's dates to 'YYYY-MM-DD' format"""
    date = date.split()
    MONTH_MAPPING = {
        "janvier": "01",
        "février": "02",
        "mars": "03",
        "avril": "04",
        "mai": "05",
        "juin": "06",
        "juillet": "07",
        "août": "08",
        "septembre": "09",
        "octobre": "10",
        "novembre": "11",
        "décembre": "12"
    }
    date[1] = MONTH_MAPPING[date[1]]
    # Pad with 0 the 1-number days' strings
    date[0] = "0" + date[0] if len(date[0]) == 1 else date[0]
    return "-".join(reversed(date))


def convert_duration(duration: str) -> Optional[int]:
    """Convert a duration string to its minutes counterpart"""
    if duration is None or not duration.endswith("min"):
        return None
    duration = duration.split()
    hours = int(duration[0].replace("h", ""))
    minutes = int(duration[1].replace("min", ""))
    return (60 * hours + minutes)


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
