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


class ItemProcessor(Protocol):
    def __call__(
        self, feedback_writer: FeedbackWriter, item: MultiprocessingExecutorPayload
    ) -> None: ...


class ItemFeedbackProcessor(Protocol):
    def __call__(self, *args: tuple, **_: dict) -> None: ...


@dataclass(frozen=True)
class MultiprocessingExecutorProperties:
    workers: int
    item_processor: ItemProcessor
    item_feedback_processor: ItemFeedbackProcessor
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


def processing_worker(
    queue: Queue, feedback_writer: FeedbackWriter, item_processor: ItemProcessor
) -> None:
    for item in get_from_queue(queue):
        item_processor(feedback_writer, item)


def feedback_worker(
    queue: Queue, item_feedback_processor: ItemFeedbackProcessor
) -> None:
    while True:
        item = queue.get()
        if isinstance(item, MultiprocessingExecutorPoisonPill):
            break
        args, kwargs = item
        item_feedback_processor(*args, **kwargs)


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
            target=feedback_worker,
            args=(self.feedback_queue, self.properties.item_feedback_processor),
        )
        feedback_process.start()

        work_processes = [
            multiprocessing_context.Process(
                target=processing_worker,
                args=(
                    self.input_queue,
                    self.feedback_writer,
                    self.properties.item_processor,
                ),
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
