import asyncio

from heartbeat import heartbeat
import time


async def wtf():
    print("WTF")


async def wth():
    print("WTH")


async def hello(**kwargs):
    print("Hello at ", time.time())


async def main():
    asyncio.create_task(wth())
    asyncio.create_task(wtf())

heartbeat.register(hello)
heartbeat.ticker()


# asyncio.get_event_loop().run_forever()
asyncio.run(main())
