import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from ahmq import Ahmq
from heartbeat import heartbeat

scheduler = AsyncIOScheduler()
scheduler.start()
ahmq = Ahmq("amqp://guest:guest@localhost", heartbeat)


async def scheduled_publish():
    await ahmq.publish("Hello skibidi rizz", "sigma", "skibidi")


async def init_rmq():
    if await ahmq.connect():
        # declare exchange
        await ahmq.declare_or_get_exchange("sigma")

if __name__ == '__main__':
    asyncio.get_event_loop().create_task(init_rmq())

    # asyncio.run(init_rmq())
    scheduler.add_job(scheduled_publish, CronTrigger(timezone="UTC", hour="*", minute="*", second="*/1"))
    asyncio.get_event_loop().run_forever()
