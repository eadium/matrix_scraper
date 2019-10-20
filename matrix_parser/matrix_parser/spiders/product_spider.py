import scrapy
import psycopg2

def init_bd(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products_extended(
        barcode bigint primary key,
        name text,
        description text,
        contents text,
        category_url text,
        mass text,
        bestbefore text,
        nutrition text,
        manufacturer text,
        image text
    );""")

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
        self.category = ''
        self.manufacturer = ''
        self.ingredients = []

    def clear_seps(self):
        if self.name is not None:
            self.name = ' '.join(self.name.split())
        if self.barcode is not None:
            self.barcode = ' '.join(self.barcode.split())
        if self.description is not None:
            self.description = ' '.join(self.description.split())
        if self.image is not None:
            self.image = ' '.join(self.image.split())
        if self.contents is not None:
            self.contents = ' '.join(self.contents.split())
        if self.mass is not None:
            self.mass = ' '.join(self.mass.split())
        if self.best_before is not None:
            self.best_before = ' '.join(self.best_before.split())
        if self.nutrition is not None:
            self.nutrition = ' '.join(self.nutrition.split())
        if self.category_url is not None:
            self.category_url = ' '.join(self.category_url.split())
        if self.manufacturer is not None:
            self.manufacturer = ' '.join(self.manufacturer.split())


        for ing in self.ingredients:
            ing['name'] = ' '.join(ing['name'].split())

    def set_csv_header(self, prod_filename, ing_filename):
        prod_file = open(prod_filename, 'a+')
        ing_file = open(ing_filename, 'a+')
        prod_header = "BARCODE;NAME;DESCRIPTION;CONTENTS;CATEGORY;MASS;BESTBEFORE;NUTRITION;MANUFACTURER;IMAGE\n"
        ing_header = "BARCODE;INGREDIENTS\n"
        prod_file.write(prod_header)
        ing_file.write(ing_header)
        prod_file.close()
        ing_file.close()

    def save_csv(self, prod_filename, ing_filename):
        prod_file = open(prod_filename, 'a+')
        ing_file = open(ing_filename, 'a+')

        prod_tuple = "{};\"{}\";\"{}\";\"{}\";\"{}\";\"{}\";\"{}\";\"{}\";\"{}\";\"{}\"".format(
            self.barcode,
            self.name,
            self.description,
            self.contents,
            self.category_url,
            self.mass,
            self.best_before,
            self.nutrition,
            self.manufacturer,
            self.image
        )
        prod_tuple = prod_tuple.replace('None', 'NULL')
        prod_tuple = prod_tuple.replace(';;', ';NULL;')

        prod_ing = ''
        for el in self.ingredients:
            prod_ing += "('{}','{}'),".format(el["name"], el["link"])
        prod_ing = prod_ing[:-1]

        ing_row = "{};{}".format(self.barcode, prod_ing)
        ing_file.write(ing_row)
        ing_file.write("\n")
        ing_file.close()

        prod_file.write(prod_tuple)
        prod_file.write("\n")
        prod_file.close()

    def save_bd(self, cursor):
        cursor.execute("""
        INSERT INTO products_extended(barcode, name, description,
            contents, category_url, mass, bestbefore, nutrition,
            manufacturer, image) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT DO NOTHING;
        """, (
            self.barcode,
            self.name,
            self.description,
            self.contents,
            self.category_url,
            self.mass,
            self.best_before,
            self.nutrition,
            self.manufacturer,
            self.image
        ))

class ProdSetSpider(scrapy.Spider):
    name = "product_spider"
    start_urls = ['http://www.goodsmatrix.ru/GMMap.aspx']
    products_urls = []
    product_counter = 0
    prod_filename = "database.csv"
    ing_filename = "ingredients.csv"

    def __init__(self):
        self.conn = psycopg2.connect(dbname='darx_db', user='manager', host='localhost')
        self.conn.set_session(autocommit=True)
        self.cursor = self.conn.cursor()

    def parse_product(self, response):
        prod = Product()
        if self.product_counter == 0:
            prod.set_csv_header(self.prod_filename, self.ing_filename)
        prod.barcode = response.xpath("//*[@id='ctl00_ContentPH_BarCodeL']/text()").get()
        if prod.barcode == None:
            return
        prod.name = response.xpath("//*[@id='ctl00_ContentPH_GoodsName']/text()").get()
        prod.description = response.xpath("//*[@id='ctl00_ContentPH_Comment']/text()").get()
        # http://www.goodsmatrix.ru/
        prod.image = response.xpath("//*[@id='ctl00_ContentPH_LSGoodPicture_GoodImg']/@src").get()
        prod.image = prod.image.replace('BigImages', 'img/products')
        prod.contents = response.xpath("//*[@id='ctl00_ContentPH_Composition']/text()").get()
        prod.mass = response.xpath("//*[@id='ctl00_ContentPH_Net']/text()").get()
        prod.nutrition = response.xpath("//*[@id='ctl00_ContentPH_ESL']/text()").get()
        prod.best_before = response.xpath("//*[@id='ctl00_ContentPH_KeepingTime']/text()").get()
        prod.category_url = response.meta.get('category_url')
        prod.manufacturer = response.meta.get('manufacturer')

        i = 0
        while True:
            ing_name = response.xpath("//*[@id='ctl00_ContentPH_Ingredients_IngrDL_ctl{:02d}_GHL']/text()".format(i)).get()
            if ing_name == None:
                break
            # if i == 0:
                # ing_name = ' '.join(ing_name.split())
            ing_link = response.xpath("//*[@id='ctl00_ContentPH_Ingredients_IngrDL_ctl{:02d}_GHL']/@href".format(i)).get()
            ing = {
                "name": ing_name,
                "link": ing_link if ing_link != None else 'NULL'
                }
            prod.ingredients.append(ing)
            i += 1
        prod.clear_seps()
        prod.save_csv(self.prod_filename, self.ing_filename)
        prod.save_bd(self.cursor)
        self.product_counter += 1


    def parse_category(self, response):
        i = 3
        category_url = ''
        categories = response.xpath("//*[@id='ctl00_ContentPH_GroupPath_GroupName']/a/text()").getall()
        for cat in categories:
            category_url += ' '.join(cat.split()) + '/'
        category_url = category_url[:-1]
        category_url = ' '.join(category_url.split())

        while True:
            # print('parsing category {}'.format(i))
            barcode = response.xpath("//*[@id='ctl00_ContentPH_GoodsDG_ctl{:02d}_A2']/text()".format(i)).get()
            manufacturer = response.xpath("//*[@id='ctl00_ContentPH_GoodsDG_ctl{:02d}_A4']/text()".format(i)).get()
            
            if barcode == None:
                break
            barcode = ' '.join(barcode.split())
            manufacturer = ' '.join(manufacturer.split())
            product_link = "http://www.goodsmatrix.ru/goods/{}.html".format(barcode)
            i += 1
            yield scrapy.Request(product_link, callback=self.parse_product, meta={
                'category_url': category_url
                })

    def parse(self, response):
        init_bd(self.cursor)
        # i_file = open('i.txt', 'a+')
        i = 2
        while True:
            # i_file.write(str(i)+'\n')
            # print('parsing category {}'.format(i))
            category_link = response.xpath("//*[@id='ctl00_ContentPH_GroupsDG_ctl{:02d}_Group']/@href".format(i)).get()
            
            if category_link == None:
                break
            if "http" not in category_link:
                category_link = "http://www.goodsmatrix.ru/{}".format(category_link)
            
            i += 1
            yield scrapy.Request(category_link, callback=self.parse_category)
        # i_file.close()
