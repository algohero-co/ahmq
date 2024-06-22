import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aio_pika.abc import AbstractIncomingMessage

from ahmq import Ahmq
from heartbeat import heartbeat

scheduler = AsyncIOScheduler()
scheduler.start()
ahmq = Ahmq("amqp://guest:guest@localhost", heartbeat)

EXCHANGE_NAME_SIGNAL = "signal"


async def scheduled_publish():
    await ahmq.publish("Hello from publisher co.algohero.signal.orig.combined_eth", EXCHANGE_NAME_SIGNAL, "co.algohero.signal.orig.combined_eth")


async def init_rmq():
    if await ahmq.connect():
        await ahmq.declare_or_get_exchange(exchange_name=EXCHANGE_NAME_SIGNAL)


async def init_subscriber():
    await ahmq.bind_and_consume(on_message, EXCHANGE_NAME_SIGNAL, "#.signal.orig.combined_eth", queue_name="combined_eth-subscriber-1")


async def on_message(message: AbstractIncomingMessage) -> None:
    async with message.process():
        print(f" [x] Received message {message.body.decode()!r}")


async def main():
    await init_rmq()
    await init_subscriber()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main())

    scheduler.add_job(scheduled_publish, CronTrigger(timezone="UTC", hour="*", minute="*", second="*/1"))
    asyncio.get_event_loop().run_forever()
