import scrapy


class PopulationSpiderSpider(scrapy.Spider):
    name = "population_spider"
    allowed_domains = ["en.wikipedia.org"]
    start_urls = ["https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"]

    def parse(self, response):
        rows = response.xpath('//table[contains(@class,"wikitable")][1]/tbody/tr')
        for row in rows:
            country = row.xpath('.//td[1]/span/a/text()').get()
            if country == None:
                continue
            population_22 = int(row.xpath('.//td[2]/text()').get().replace(',','')) # убираем запятые --> int
            population_23 = int(row.xpath('.//td[3]/text()').get().replace(',',''))
            change = float(row.xpath('.//td[4]/span/text()').get()[:-1].replace('−','-')) #срез %  замена  черточки на минус --> float
            region = row.xpath('.//td[5]/a/text()').get()
            sub_region = row.xpath('.//td[6]/a/text()').get()
            yield {
                'country': country,
                'population_22': population_22,
                'population_23': population_23,
                'change': change,
                'region': region,
                'sub_region': sub_region,
                }
            
            
#scrapy genspider population_spider "en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"

#scrapy crawl population_spider

#scrapy crawl population_spider -o population.json