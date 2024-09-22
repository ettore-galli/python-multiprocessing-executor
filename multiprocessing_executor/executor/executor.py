from __future__ import annotations

import multiprocessing
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import partial
from multiprocessing import Queue
from typing import Any


class PoisonPill:
    pass


class Payload:
    pass


FeedbackWriter = Callable[[Any], None]

ProcessingWorker = Callable[[Queue], None]

FeedbackWorker = Callable[[Queue], None]

ItemProcessor = Callable[[FeedbackWriter, Any], None]

ItemFeedbackProcessor = Callable[[Any], None]


@dataclass(frozen=True)
class MultiprocessingExecutorProperties:
    workers: int
    item_processor: ItemProcessor
    item_feedback_processor: ItemFeedbackProcessor
    task_source: Iterable[Any]


def processing_worker(
    queue: Queue, feedback_writer: FeedbackWriter, item_processor: ItemProcessor
) -> None:
    while True:
        item = queue.get()
        if isinstance(item, PoisonPill):
            break
        item_processor(feedback_writer, item)


def feedback_worker(
    queue: Queue, item_feedback_processor: ItemFeedbackProcessor
) -> None:
    while True:
        item = queue.get()
        if isinstance(item, PoisonPill):
            break
        args, kwargs = item
        item_feedback_processor(*args, **kwargs)


def feedback_writer(queue: Queue, *args: tuple, **kwargs: dict) -> None:
    queue.put((args, kwargs))


class MultiprocessingExecutor:
    def __init__(self, properties: MultiprocessingExecutorProperties) -> None:

        self.input_queue: Queue = Queue()
        self.feedback_queue: Queue = Queue()
        self.feedback_writer = partial(feedback_writer, self.feedback_queue)

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
            self.input_queue.put(PoisonPill())
        self.input_queue.close()

        for process in work_processes:
            process.join()

        self.feedback_queue.put(PoisonPill())
