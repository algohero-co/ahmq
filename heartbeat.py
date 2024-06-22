import asyncio
from typing import Callable

__all__ = "heartbeat"

import uuid


class HeartBeat(object):
    """A class that acts like a heartbeat, with an interval of 1 second between each beat.
    During every beat, the registered tasks will be evaluated and possibly run,
    depending on each task's interval.

    This class is useful in some cases, such as checking whether a connection to
    a service is alive every 60 seconds, or logging the current service
    status.
    """

    def __init__(self):
        self._count = 0  # number of beats
        self._interval = 1  # beat execution interval (seconds)
        self._tasks = {}  # list of callback tasks executed following the heartbeat

    @property
    def count(self):
        return self._count

    def ticker(self):
        """Call this method to start the beat."""
        asyncio.get_event_loop().call_later(self._interval, self.ticker)  # set next beat

        for task_id, task in self._tasks.items():
            interval = task["interval"]
            if self._count % interval != 0:
                continue
            func = task["func"]
            args = task["args"]
            kwargs = task["kwargs"]
            kwargs["task_id"] = task_id
            kwargs["heart_beat_count"] = self._count
            asyncio.get_event_loop().create_task(func(*args, **kwargs))

    def register(self, func: Callable, interval=1, *args, **kwargs) -> uuid.UUID:
        """Register a task and call it at each heartbeat.

        Args:
            func: The function executed at the heartbeat
            interval: The interval between callback executions (seconds)

        Returns:
            Task id.
        """
        t = {"func": func,
             "interval": interval,
             "args": args,
             "kwargs": kwargs}
        task_id = uuid.uuid1()  # random uuid
        self._tasks[task_id] = t
        return task_id

    def unregister(self, task_id):
        """Cancel a task.

        Args:
            task_id: Task to cancel
        """
        if task_id in self._tasks:
            self._tasks.pop(task_id)


heartbeat = HeartBeat()
