#!/usr/bin/env python3
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from tasks import topdeal
from tasks import product
import time
import random
_executor = ThreadPoolExecutor(1)

logger = logging.getLogger(__name__)


async def main():
    loop = asyncio.get_event_loop()
    logging.info('Start to crawl....')

    for i in range(1, 64):
        logging.info('send job to worker %d' % i)
        await loop.run_in_executor(_executor, topdeal.delay, i)

    logging.info('finish  crawl....')
    time.sleep(random.randint(900, 1200))
    logging.info('Start to parse product....')

    for i in range(10):
        await loop.run_in_executor(_executor, product.delay)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
