# -*- coding: utf-8 -*-
from celery import Celery
from celery.utils.log import get_task_logger
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from parsel import Selector
import redis
import traceback


app = Celery('tasks',  backend='redis://127.0.0.1:6379/0',
             broker='redis://127.0.0.1:6379/1')

pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=2)
r = redis.Redis(connection_pool=pool)

chrome_options = Options()
chrome_options.add_argument('--lang=en_US')
chrome_options.add_argument('--window-size=1920,2560')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-extensions')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('blink-settings=imagesEnabled=false')
chrome_options.add_argument('--headless')
browser = webdriver.Chrome('chromedriver', chrome_options=chrome_options)

log = get_task_logger(__name__)


@app.task
def topdeal(i):
    log.info('Start to parse topdeal.....')
    time.sleep(random.randint(5, 10))
    url = 'https://www.amazon.com/international-sales-offers/b/ref=gbps_ftr_m-9_cb51_page_'
    url += str(i)
    url += '?node=15529609011&gb_f_deals1=dealStates:AVAILABLE%252CWAITLIST%252CWAITLISTFULL%252CEXPIRED%252CSOLDOUT%252CUPCOMING,page:'
    url += str(i)
    url += ',sortOrder:BY_SCORE,MARKETING_ID:ship_export,dealsPerPage:16&pf_rd_p=00eb8327-d9b6-4435-9348-e71f4359cb51&pf_rd_s=merchandised-search-9&pf_rd_t=101&pf_rd_i=15529609011&pf_rd_m=ATVPDKIKX0DER&pf_rd_r=1YA6RAFSG05B50YKT633&ie=UTF8'
    log.info('got page url  -> %s' % url)
    browser.get(url)
    sel = Selector(text=browser.page_source)

    for j in range(16):
        tmp = '//*[@id="100_dealView_'
        tmp += str(j)
        tmp += '"]/div/div[2]/div/a/@href'

        try:
            value = sel.xpath(tmp).get()
        except ValueError:
            log.info('got ValueError value  -> %s' % value)
        except:
            log.debug('got unexpected error')
            traceback.print_exc()

        log.info('got product url  -> %s' % value)

        if value is not None:
            r.sadd('producturls', value)
            log.info('save producturls  -> %s' % value)

    log.info('finish to parse topdeal.....')


@app.task
def product():
    log.info('Start to parse product.....')
    time.sleep(random.randint(5, 10))

    while True:
        try:
            url = str(r.spop('producturls'), 'utf-8')
            log.info('load producturls  -> %s' % url)
            browser.get(url)
            sel = Selector(text=browser.page_source)

            try:
                title = sel.xpath('//*[@id="productTitle"]/text()').get()
                r.sadd('producttitle', title)
                log.info('save product title  -> %s' % title)
            except ValueError:
                log.info('got ValueError value  -> %s' % title)
            except:
                log.debug('parse html got unexpected error')
                traceback.print_exc()

        except TypeError:
            log.info('no more url to parse, finish the job')
            break
        except:
            log.debug('redis got unexpected error')
            traceback.print_exc()
            break
