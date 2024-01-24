from bs4 import BeautifulSoup
import requests
import pandas as pd 
import sqlite3
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium import webdriver
from multiprocessing.pool import ThreadPool as Pool
import multiprocessing
import threading
from time import sleep




class OLX:
        def __init__(self, queue, lock):
            self.queue = queue
            self.lock = lock
            self.all_data = []
            self.finish_first = False  

            options = Options()
            options.add_argument('--headless')

            self.driver = webdriver.Firefox(options=options)
        def requests_olx(self, url):
            self.driver.get(url)
            return self.driver.page_source
        
        def anuncios_pg(self, url):
            pg = 0

            while True:
                pg+=1
                print('PAGE: ', pg)
                content_pg = self.requests_olx(url)
                source_pg = BeautifulSoup(content_pg, 'html.parser')
                list_anun = list(map(lambda x: x.attrs['href'], source_pg.find_all('a', attrs={'data-ds-component':'DS-NewAdCard-Link'})))
                self.lock.acquire()   
                [self.queue.put(i) for i in list_anun]
                self.lock.release()
                self.finish_first = True


                if source_pg.select_one('a[data-ds-component="DS-Button"].olx-button.olx-button--link-button.olx-button--small.olx-button--a:nth-of-type(12)') or pg > 5:
                    url = source_pg.select_one('a[data-ds-component="DS-Button"].olx-button.olx-button--link-button.olx-button--small.olx-button--a:nth-of-type(12)').get('href')

                else:
                    print('acabou de pegar os links...')
                    break

        def anuncios(self, id_robot=None, url=None):
            while True:
                 sleep(1)
                 if self.finish_first == True:
                    url = self.queue.get(block = True)
                    break
            while True:


                content_pg = self.requests_olx(url)
                source_pg = BeautifulSoup(content_pg, 'html.parser')
                values = {}
                try:
                    title_span = source_pg.find('span', class_='olx-text--title-medium')
                    values['title'] =  title_span.text.strip() if title_span else None

                    owner_name_span = source_pg.find('span', class_='olx-text olx-text--body-large olx-text--block olx-text--regular ad__sc-ypp2u2-4 TTTuh')
                    values['owner_name'] = owner_name_span.text if owner_name_span else None

                    description_span = source_pg.find('meta', {'property':'og:description'})
                    values['description'] = description_span.get('content') if description_span else None

                    url_anuncio = source_pg.find('meta', {'property':'og:url'})
                    url_id = url_anuncio.get('content') if url_anuncio else None
                                
                    values['id'] = url_id.split('-')[-1]

                    adress_span = source_pg.find('span', class_='olx-text olx-text--body-small olx-text--block olx-text--semibold olx-color-neutral-110')
                    adress_text = adress_span.text.strip() if adress_span else None
                    v = adress_text.split(',') if adress_text else None
                    values['city'] = v[0].strip() if adress_text else None
                    values['state'] = v[1].strip() if adress_text else None
                    values['cep'] = v[2].strip() if adress_text else None

                except:
                    print("***",url)

                print('Proc_id: {} | link: {}'.format(id_robot, url))

                self.lock.acquire()   
                self.all_data.append(values)
                self.lock.release()
                
                if self.queue.qsize() == 0:
                    break
                else:
                    url = self.queue.get(block = True)
                

            
           
        
        def __del__(self):
            self.driver.quit()


if __name__ == '__main__':

    num_robot = 4
    pool = Pool(num_robot)

    manager = multiprocessing.Manager()
    queue = manager.Queue()
    lock = multiprocessing.Lock()

    threads = []
    olx = OLX(queue, lock)

    add_itens = threading.Thread(target=olx.anuncios_pg, args=('https://www.olx.com.br/servicos?sct=9',))
    add_itens.start()

    for i in range(1,num_robot):
        threads.append(pool.apply_async(olx.anuncios() , args=(i, None)))
    for result in threads:
        result.get()
    pool.close()
    pool.join()
    add_itens.join()

res = olx.anuncios_pg('https://www.olx.com.br/servicos?sct=9')
con = 0
for i in res:
    con+=1
    print(olx.anuncios(i))
    if con > 5:
        break
df = pd.DataFrame(OLX.all_data)
df