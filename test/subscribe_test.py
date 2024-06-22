import asyncio

from aio_pika.abc import AbstractIncomingMessage

from ahmq import Ahmq
from heartbeat import heartbeat


async def on_message(message: AbstractIncomingMessage) -> None:
    async with message.process():
        print(f" [x] Received message {message.body.decode()!r}")


async def init_rmq():
    ahmq = Ahmq("amqp://guest:guest@localhost", heartbeat)
    if await ahmq.connect():
        # declare exchange
        await ahmq.declare_or_get_exchange("sigma")

        await ahmq.bind_and_consume(on_message, "sigma", "skibidi", "rizz")

if __name__ == '__main__':
    asyncio.get_event_loop().create_task(init_rmq())
    asyncio.get_event_loop().run_forever()
