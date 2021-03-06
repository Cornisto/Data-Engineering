from string import ascii_lowercase
import scrapy
from scrapy.crawler import CrawlerProcess
import json


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
                      'event_name': response.xpath('//h2[@class="b-content__title"]/'
                                                   'span[@class="b-content__title-highlight"]'
                                                   '/text()').extract_first().strip()
                      }

        for info in response.xpath('//ul[@class="b-list__box-list"]/li[@class="b-list__box-list-item"]'):
            info_name = info.css('i::text').extract_first().strip().replace(':', '').lower()
            info_values = [info_val.strip() for info_val in info.css('*::text').extract() if
                           info_val.strip().replace(':', '').lower() != info_name and info_val.strip() != '']
            info_value = info_values[0] if info_values else ''
            event_info[info_name] = info_value

        for fight_url in response.xpath('//tr[@class="b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click"]'
                                        '/@data-link').extract():
            yield scrapy.Request(fight_url, callback=self.parse_fight_info, meta={'fight_info': event_info,
                                                                                  'fight_url': fight_url})

    def parse_fight_info(self, response):

        fight_info = response.meta.get('fight_info')
        fight_info['fight_url'] = response.meta.get('fight_url')

        fighters_info = {}
        fighters = [None, None]

        for fighter in response.xpath('//div[@class="b-fight-details__person"]'):
            fighter_name = fighter.xpath(
                './/div[@class="b-fight-details__person-text"]/h3/a/text()').extract_first().strip()
            fighters_info[fighter_name] = {'profile_url': fighter.xpath(
                './/div[@class="b-fight-details__person-text"]/h3/a/@href').extract_first().strip(),
                                           'result': fighter.xpath('.//i/text()').extract_first().strip()
                                           }
            fighters[1] = fighter_name

        fighters[0] = [fighter for fighter in fighters_info.keys() if fighter != fighters[1]][0]

        fight_info['bout'] = response.xpath('//i[@class="b-fight-details__fight-title"]/text()').extract_first().strip()

        stat_names = [s.replace(':', '').strip().lower() for s in
                      response.xpath('//div[@class="b-fight-details__content"]'
                                     '/p/i/i[@class="b-fight-details__label"]/text()').extract()]

        stat_values = []

        for stat in response.xpath('//div[@class="b-fight-details__content"]/p/i'):
            stat_value = ''
            try:
                stat_value = stat.xpath('.//i/following-sibling::text()').extract_first().strip()
            except AttributeError:
                pass
            if stat_value == '':
                try:
                    stat_value = stat.xpath('.//i/following-sibling::node()/text()').extract_first().strip()
                except AttributeError:
                    stat_vals = [stat_val.strip() for stat_val in
                                   stat.xpath('.//ancestor::node()[1]/text()').extract()
                                   if stat_val.strip().replace(':', '').lower() not in stat_names and stat_val.strip() != '']
                    if stat_vals:
                        stat_value = stat_vals[0]
                    else:
                        stat_value = ', '.join([stat_val.strip() for stat_val in stat.xpath('./following-sibling::node()/text()').extract()
                                                if stat_val.strip() != '' and stat_val.strip() not in stat_values]).replace('.', '')
            stat_values.append(stat_value)

            if len(stat_values) == len(stat_names):
                break

        for i in range(len(stat_names)):
            fight_info[stat_names[i]] = stat_values[i]

        stat_headers = []
        for header_tbl in response.xpath('//table[not (@class)]'):
            headers = [header.replace('.', ' ').strip().lower() for header in
                       header_tbl.xpath('.//thead[@class="b-fight-details__table-head"]'
                                        '/tr/th[@class="b-fight-details__table-col"]/text()').extract() if header.strip() != '']
            stat_headers.append(headers)

        stat_tables = response.xpath('//section[@class="b-fight-details__section js-fight-section"]/'
                                     'table[@class="b-fight-details__table js-fight-table"]')
        round_headers = stat_tables[0].xpath('.//thead[@class="b-fight-details__table-row b-fight-details__table-row_type_head"]')
        
        table_num = 0
        for stat_tbl in response.xpath('//section[@class="b-fight-details__section js-fight-section"]/table[@class="b-fight-details__table js-fight-table"]'):
            round_num = 0
            stat_num = 0
            for round_stats in stat_tbl.xpath('.//tbody/tr[@class="b-fight-details__table-row"]/td[@class="b-fight-details__table-col"]'):
                fighters_stats = round_stats.xpath('.//p/text()').extract()
                for i in range(len(fighters_stats)):
                    fighters_info[fighters[i]][stat_headers[table_num][stat_num]] = fighters_stats[i].strip()
                stat_num += 1
                if stat_num == len(stat_headers[table_num]):
                    fighters_info[fighters[0]]['round'] = round_num + 1
                    fighters_info[fighters[1]]['round'] = round_num + 1
                    with open('ufc_data/events.json', 'a+') as f:
                        json.dump({**fight_info, **fighters_info[fighters[0]]}, f)
                        f.write(',\n')
                        json.dump({**fight_info, **fighters_info[fighters[1]]}, f)
                        f.write(',\n')
                    round_num += 1
                    stat_num = 0
            table_num += 1


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(EventsInfoSpider)
    process.crawl(FighterInfoSpider)
    process.start()
