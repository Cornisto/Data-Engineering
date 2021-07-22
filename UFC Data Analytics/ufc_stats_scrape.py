from string import ascii_lowercase
import scrapy
from scrapy.crawler import CrawlerProcess


class FighterInfoSpider(scrapy.Spider):
    name = 'fighter_info'

    custom_settings = {
        "FEEDS": {
            "ufc_data/fighters.json": {"format": "json"}
        }
    }

    def start_requests(self):
        urls = [f'http://www.ufcstats.com/statistics/fighters?char={letter}&page=all' for letter in ascii_lowercase]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        fighter_profiles = response.css(
            'table[class=b-statistics__table] \
            tbody tr[class=b-statistics__table-row] \
            td[class=b-statistics__table-col] a::attr(href)').getall()
        for profile_url in fighter_profiles:
            yield scrapy.Request(profile_url, callback=self.parse_fighter_info, meta={'request_url': profile_url})

    def parse_fighter_info(self, response):
        fighter_info = {'profile_url': response.meta.get('request_url'),
                        'fighter_name': response.css('h2[class=b-content__title] '
                                                     'span[class=b-content__title-highlight]::text').extract_first().strip(),
                        'fighter_record': response.css('h2[class=b-content__title] '
                                                       'span[class=b-content__title-record]::text').extract_first().strip(),
                        'fighter_nickname': response.css('p[class=b-content__Nickname]::text').extract_first().strip()
                        }

        for stat in response.css('ul[class=b-list__box-list] li'):
            stat_name = stat.css('i::text').extract_first().strip().replace(':', '').lower()
            stat_values = [stat_val.strip() for stat_val in stat.css('*::text').extract() if
                           stat_val.strip().replace(':', '').lower() != stat_name and stat_val.strip() != '']
            stat_value = stat_values[0] if stat_values else ''
            fighter_info[stat_name] = stat_value

        yield fighter_info


class EventsInfoSpider(scrapy.Spider):
    name = 'events_info'

    custom_settings = {
        "FEEDS": {
            "ufc_data/events.json": {"format": "json"}
        }
    }

    def start_requests(self):
        url = 'http://www.ufcstats.com/statistics/events/completed?page=all'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        event_urls = response.xpath('//table[@class="b-statistics__table-events"]/tbody/'
                                    'tr[@class="b-statistics__table-row"]/td[@class="b-statistics__table-col"]/'
                                    'i[@class="b-statistics__table-content"]/a/@href').getall()

        for url in event_urls:
            self.log(f'Processing data for event: {url}')
            yield scrapy.Request(url, callback=self.parse_event_info, meta={'request_url': url})

    def parse_event_info(self, response):

        request_url = response.meta.get('request_url')

        event_info = {'event_url': request_url,
                      'event_name': response.xpath('//h2[@class="b-content__title"]/span['
                                                   '@class="b-content__title-highlight"]/text()').extract_first().strip()
                      }

        for info in response.xpath('//ul[@class="b-list__box-list"]/li[@class="b-list__box-list-item"]'):
            info_name = info.css('i::text').extract_first().strip().replace(':', '').lower()
            info_values = [info_val.strip() for info_val in info.css('*::text').extract() if
                           info_val.strip().replace(':', '').lower() != info_name and info_val.strip() != '']
            info_value = info_values[0] if info_values else ''
            event_info[info_name] = info_value

        yield event_info


if __name__ == "__main__":

    process = CrawlerProcess()
    process.crawl(EventsInfoSpider)
    process.crawl(FighterInfoSpider)
    process.start()
