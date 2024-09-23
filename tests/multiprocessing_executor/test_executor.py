import os
from collections.abc import Generator
from functools import partial
from multiprocessing import Queue
from pathlib import Path
from tempfile import TemporaryDirectory

from multiprocessing_executor.executor.executor import (
    FeedbackWriter,
    MultiprocessingExecutor,
    MultiprocessingExecutorPayload,
    MultiprocessingExecutorProperties,
    get_from_queue,
)


class ExampleExecutorPayload(MultiprocessingExecutorPayload):
    def __init__(self, item: str) -> None:
        super().__init__()
        self.item = item


def processing_worker(
    outfile: Path,
    queue: Queue,
    feedback_writer: FeedbackWriter,
) -> None:
    with Path.open(outfile, "w", encoding="utf-8") as workf:
        for item in get_from_queue(queue):
            processed_item = f"Processed: {item}"
            workf.write(processed_item)
            feedback_writer(processed_item)


def feedback_worker(logfile: Path, queue: Queue) -> None:
    with Path.open(logfile, "w", encoding="utf-8") as logf:
        for item in get_from_queue(queue):
            log_message = f"Feedback for: {item}"
            logf.write(log_message)


def test_multiprocessing_executor_process() -> None:
    tmpdir = TemporaryDirectory("multi-processing")

    def data_source() -> Generator[ExampleExecutorPayload, None, None]:
        for index in range(10):
            yield ExampleExecutorPayload(item=f"test_input_{index + 1}")

    logfile = Path(tmpdir.name, "logfile.txt")
    outfile = Path(tmpdir.name, "outfile.txt")

    properties = MultiprocessingExecutorProperties(
        workers=3,
        processing_worker=partial(processing_worker, outfile),
        feedback_worker=partial(feedback_worker, logfile),
        task_source=data_source(),
    )

    executor = MultiprocessingExecutor(properties=properties)
    executor.perform()

    files_list = os.listdir(tmpdir.name)
    assert sorted(files_list) == sorted(["logfile.txt", "outfile.txt"])


if __name__ == "__main__":
    test_multiprocessing_executor_process()
