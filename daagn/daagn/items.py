# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DaagnItem(scrapy.Item):
    product_name = scrapy.Field()
    rental_price = scrapy.Field()
    post_link = scrapy.Field()
    category = scrapy.Field()
    rental_duration = scrapy.Field()
