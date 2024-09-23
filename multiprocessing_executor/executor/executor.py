from __future__ import annotations

import multiprocessing
from collections.abc import Callable, Generator, Iterable
from dataclasses import dataclass
from functools import partial
from multiprocessing import Queue
from typing import Any, Protocol, TypeVar


class MultiprocessingExecutorPoisonPill:
    pass


class MultiprocessingExecutorPayload:
    pass


FeedbackWriter = Callable[[Any], None]


class ProcessingWorker(Protocol):
    def __call__(self, queue: Queue) -> None: ...


class FeedbackWorker(Protocol):
    def __call__(self, queue: Queue) -> None: ...


@dataclass(frozen=True)
class MultiprocessingExecutorProperties:
    workers: int
    processing_worker: ProcessingWorker
    feedback_worker: FeedbackWorker
    task_source: Iterable[Any]


QC = TypeVar("QC")


def get_from_queue(
    queue: Queue[QC],
) -> Generator[QC, None, None]:
    while True:
        item: QC = queue.get()
        if isinstance(item, MultiprocessingExecutorPoisonPill):
            break
        yield item


def feedback_writer_template(queue: Queue, *args: tuple, **kwargs: dict) -> None:
    queue.put((args, kwargs))


class MultiprocessingExecutor:
    def __init__(self, properties: MultiprocessingExecutorProperties) -> None:

        self.input_queue: Queue = Queue()
        self.feedback_queue: Queue = Queue()
        self.feedback_writer: FeedbackWriter = partial(
            feedback_writer_template, self.feedback_queue
        )

        self.properties: MultiprocessingExecutorProperties = properties

    def perform(self) -> None:

        multiprocessing_context = multiprocessing.get_context("spawn")

        feedback_process = multiprocessing_context.Process(
            target=self.properties.feedback_worker,
            args=(self.feedback_queue,),
        )

        feedback_process.start()

        work_processes = [
            multiprocessing_context.Process(
                target=self.properties.processing_worker,
                args=(self.input_queue, self.feedback_writer),
            )
            for _ in range(self.properties.workers)
        ]

        for process in work_processes:
            process.start()

        for item in self.properties.task_source:
            self.input_queue.put(item)

        for _ in range(self.properties.workers):
            self.input_queue.put(MultiprocessingExecutorPoisonPill())
        self.input_queue.close()

        for process in work_processes:
            process.join()

        self.feedback_queue.put(MultiprocessingExecutorPoisonPill())
