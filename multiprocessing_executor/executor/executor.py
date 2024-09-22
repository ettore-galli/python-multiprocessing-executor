from __future__ import annotations

import multiprocessing
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from multiprocessing import Queue
from typing import Any


class PoisonPill:
    pass


class Payload:
    pass


ProcessingWorker = Callable[[Queue], None]

FeedbackWorker = Callable[[Queue], None]


@dataclass(frozen=True)
class MultiprocessingExecutorProperties:
    workers: int
    processing_worker: ProcessingWorker
    feedback_worker: FeedbackWorker
    task_source: Iterable[Any]


class MultiprocessingExecutor:
    def __init__(self, properties: MultiprocessingExecutorProperties) -> None:
        self.properties: MultiprocessingExecutorProperties = properties

        self.input_queue: Queue = Queue()
        self.feedback_queue: Queue = Queue()

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
                args=(self.input_queue,),
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
