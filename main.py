import asyncio
import json
import random
import time
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
BACKUP_WAIT_TIME = float(os.getenv('BACKUP_WAIT_TIME', '30'))

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

async def get_status(client: httpx.AsyncClient, command: str) -> bool:
    r = await client.get(CLICKHOUSE_BACKUP_ADDR + '/backup/status')
    logging.info('received status: %s', r.text)
    docs = []
    for line in r.text.splitlines():
        doc = json.loads(line)
        docs.append(doc)

    assert len(docs) == 1
    doc = docs[0]
    if doc['command'] != command:
        return False

    return doc['status'] == 'success'

async def wait_status(client: httpx.AsyncClient, command: str, timeout: float = 30) -> bool:
    started_at = time.time()
    elapsed = 0
    while True:
        status = await get_status(client, command)
        if status:
            return True
        await asyncio.sleep(2.0)
        elapsed = time.time() - started_at
        if elapsed >= timeout:
            return False
    return False
        

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
                    raise Exception('backup failed')

                if not await wait_status(client, 'create', BACKUP_WAIT_TIME):
                    raise Exception('failed waiting for status for create')
                
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
                    break

                if not success:
                    raise Exception('upload failed')
                else:
                    if not await wait_status(client, f'upload {name}', BACKUP_WAIT_TIME):
                        raise Exception('failed waiting for status for upload')
        except Exception as e:
            logging.exception(e)
            METRICS_BACKUP_ERROR.inc()
        else:
            METRICS_BACKUP_SUCCESS.inc()
            logging.info('backup finished successfully')

    try:
        while True:
            await asyncio.sleep(3600)
    except:
        await runner.cleanup()

asyncio.run(main())
