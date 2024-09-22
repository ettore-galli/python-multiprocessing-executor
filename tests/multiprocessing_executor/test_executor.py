import os
from collections.abc import Generator
from functools import partial
from multiprocessing import Queue
from pathlib import Path
from tempfile import TemporaryDirectory

from multiprocessing_executor.executor.executor import (
    MultiprocessingExecutor,
    MultiprocessingExecutorProperties,
    PoisonPill,
)


def proecssing_worker(tempdir: str, queue: Queue) -> None:
    while True:
        item = queue.get()
        if isinstance(item, PoisonPill):
            break
        processed_item = "Processed: {item}"
        outfile = Path(tempdir, item)
        with Path.open(outfile, "w", encoding="utf-8") as workf:
            workf.write(processed_item)


def feedback_worker(tempdir: str, queue: Queue) -> None:
    while True:
        item = queue.get()
        if isinstance(item, PoisonPill):
            break
        item_feedback = "Feedback of: {item}"
        outfile = Path(tempdir, f"{item}_feedback")
        with Path.open(outfile, "w", encoding="utf-8") as feedbackf:
            feedbackf.write(item_feedback)


def test_multiprocessing_executor_process() -> None:
    tmpdir = TemporaryDirectory("multi-processing")

    def data_source() -> Generator[str, None, None]:
        for index in range(10):
            yield f"test_input_{index + 1}"

    properties = MultiprocessingExecutorProperties(
        workers=3,
        processing_worker=partial(proecssing_worker, tmpdir.name),
        feedback_worker=partial(feedback_worker, tmpdir.name),
        task_source=data_source(),
    )

    executor = MultiprocessingExecutor(properties=properties)
    executor.perform()

    files_list = os.listdir(tmpdir.name)
    assert sorted(files_list) == sorted(
        [
            "test_input_1",
            "test_input_2",
            "test_input_3",
            "test_input_4",
            "test_input_5",
            "test_input_6",
            "test_input_7",
            "test_input_8",
            "test_input_9",
            "test_input_10",
        ]
    )


if __name__ == "__main__":
    test_multiprocessing_executor_process()
