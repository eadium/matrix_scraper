import scrapy

class Product:
    def __init__(self):
        self.name = '' 
        self.barcode = ''
        self.description = '' 
        self.image = '' 
        self.contents = '' 
        self.mass = '' 
        self.best_before = ''
        self.nutrition = '' 
        self.ingredients = [] 
    
    def save_csv(self, prod_filename, ing_filename):
        prod_file = open(prod_filename, 'a+')
        ing_file = open(ing_filename, 'a+')

        prod_tuple = "{};{};{};{};{};{};{};{}".format(
            self.name, 
            self.barcode, 
            self.description,
            self.contents,
            self.mass,
            self.best_before,
            self.nutrition,
            self.image
        )
        prod_tuple = prod_tuple.replace('None', 'NULL')

        prod_ing = ''
        for el in self.ingredients:
            prod_ing += "({},{}),".format(el["name"], el["link"])
        prod_ing = prod_ing[:-1]

        ing_row = "{};{}".format(self.barcode, prod_ing)
        ing_file.write(ing_row)
        ing_file.write("\n")
        ing_file.close()

        prod_file.write(prod_tuple)
        prod_file.write("\n")
        prod_file.close()

    # def save_bd():


class ProdSetSpider(scrapy.Spider):
    name = "product_spider"
    start_urls = ['http://www.goodsmatrix.ru/goods/4601738005594.html']

    def parse(self, response):
        prod = Product()
        prod.name = response.xpath("//*[@id='ctl00_ContentPH_GoodsName']/text()").get()
        prod.barcode = response.xpath("//*[@id='ctl00_ContentPH_BarCodeL']/text()").get()
        prod.description = response.xpath("//*[@id='ctl00_ContentPH_Comment']/text()").get()
        prod.image = 'http://www.goodsmatrix.ru/' + response.xpath(
            "//*[@id='ctl00_ContentPH_LSGoodPicture_GoodImg']/@src").get()
        prod.contents = response.xpath("//*[@id='ctl00_ContentPH_Composition']/text()").get()
        prod.mass = response.xpath("//*[@id='ctl00_ContentPH_Net']/text()").get()
        prod.nutrition = response.xpath("//*[@id='ctl00_ContentPH_ESL']/text()").get()
        prod.best_before = response.xpath("//*[@id='ctl00_ContentPH_KeepingTime']/text()").get()
            
        i = 0
        while True:
            ing_name = response.xpath("//*[@id='ctl00_ContentPH_Ingredients_IngrDL_ctl{:02d}_GHL']/text()".format(i)).get()
            if ing_name == None:
                break
            if i == 0:
                ing_name = ing_name[2:]
            ing_link = response.xpath("//*[@id='ctl00_ContentPH_Ingredients_IngrDL_ctl{:02d}_GHL']/@href".format(i)).get()
            ing = {
                "name": ing_name, 
                "link": ing_link if ing_link != None else 'NULL'
                }
            prod.ingredients.append(ing)
            i += 1
        
        prod.save_csv("database.csv", "ingredients.csv")