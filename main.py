import asyncio
from random import random
import aiocron
import os
import logging
import httpx
from prometheus_client import Counter
import prometheus_client
from aiohttp import web

METRICS_BACKUP_SUCCESS = Counter("chb_backup_success", "number of successful backups")
METRICS_BACKUP_ERROR = Counter("chb_backup_error", "number of errored backups")

logging.basicConfig(
    level=logging.DEBUG, 
    format="%(created)f %(asctime)s.%(msecs)03d [%(process)d] "
        "[%(name)s::%(module)s:%(funcName)s:%(lineno)d] "
        "%(levelname)s: %(message)s"
)

CLICKHOUSE_BACKUP_ADDR = os.getenv('CLICKHOUSE_BACKUP_ADDR', 'http://127.0.0.1:7171')
BACKUP_SCHEDULE = os.getenv('BACKUP_SCHEDULE', '* * */24 * *')

async def metrics(request):
    latest = prometheus_client.generate_latest()
    resp = web.Response(body=latest.decode())
    resp.content_type = prometheus_client.CONTENT_TYPE_LATEST
    return resp

app = web.Application()
app.add_routes([
    web.get('/metrics', metrics)
])

logging.info('starting cron at schedule: %s', BACKUP_SCHEDULE)

async def main():
    runner = web.AppRunner(app, handle_signals=True)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 1337)
    await site.start()

    @aiocron.crontab(BACKUP_SCHEDULE)
    async def attime():
        logging.info('running backup')
        try:

            async with httpx.AsyncClient() as client:
                r = await client.post(CLICKHOUSE_BACKUP_ADDR + '/backup/create')
                backup = r.json()
                logging.info('created backup: %s', backup)
                if backup['status'] != 'acknowledged':
                    return
                
                name = backup['backup_name']

                success = False
                for _ in range(5):
                    r = await client.post(CLICKHOUSE_BACKUP_ADDR + f'/backup/upload/{name}')
                    upload = r.json()
                    if upload['status'] == 'error':
                        logging.warning('received error on upload: %s', upload)
                        await asyncio.sleep(random.random() * 5)
                        continue

                    success = True
                    logging.info('uploaded backup: %s', upload)

                if not success:
                    logging.error('upload failed')
        except Exception as e:
            logging.exception(e)
            METRICS_BACKUP_ERROR.inc()
        else:
            METRICS_BACKUP_SUCCESS.inc()

    try:
        while True:
            await asyncio.sleep(3600)
    except:
        await runner.cleanup()

asyncio.run(main())
