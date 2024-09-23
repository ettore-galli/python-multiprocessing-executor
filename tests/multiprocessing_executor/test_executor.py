import os
from collections.abc import Generator
from enum import Enum
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


class FeedbackType(Enum):
    A = "A"
    B = "B"


def processing_worker(
    tmpdir: Path,
    queue: Queue,
    feedback_writer: FeedbackWriter,
) -> None:

    payload: ExampleExecutorPayload

    def write_feedback_a(feedback: str) -> None:
        feedback_writer((FeedbackType.A, f"{FeedbackType.A}{feedback}"))

    def write_feedback_b(feedback: str) -> None:
        feedback_writer((FeedbackType.B, f"{FeedbackType.B}{feedback}"))

    for payload in get_from_queue(queue):
        content = payload.item
        outfile = Path(tmpdir, f"{content}-outfile.txt")
        with Path.open(outfile, "w", encoding="utf-8") as workf:
            processed_item = f"Processed: {payload}"
            workf.write(processed_item)
            write_feedback_a(processed_item)
            write_feedback_b(processed_item)


def feedback_worker(tmpdir: Path, queue: Queue) -> None:
    outfile_a = Path(tmpdir, "logfile-a.txt")
    outfile_b = Path(tmpdir, "logfile-b.txt")

    with (
        Path.open(outfile_a, "w", encoding="utf-8") as logf_a,
        Path.open(outfile_b, "w", encoding="utf-8") as logf_b,
    ):
        strategy = {
            FeedbackType.A: logf_a,
            FeedbackType.B: logf_b,
        }
        for payload in get_from_queue(queue):
            feedback_type, feedback_message = payload
            log_message = f"Feedback for: {feedback_message}\n"
            strategy.get(feedback_type, logf_a).write(log_message)


def test_multiprocessing_executor_process() -> None:
    tmpdir = TemporaryDirectory("multi-processing")

    data_size = 10

    def data_source() -> Generator[ExampleExecutorPayload, None, None]:
        for index in range(data_size):
            yield ExampleExecutorPayload(item=f"test_input_{index + 1}")

    properties = MultiprocessingExecutorProperties(
        workers=3,
        processing_worker=partial(processing_worker, Path(tmpdir.name)),
        feedback_worker=partial(feedback_worker, Path(tmpdir.name)),
        task_source=data_source(),
    )

    executor = MultiprocessingExecutor(properties=properties)
    executor.perform()

    files_list = os.listdir(tmpdir.name)
    assert sorted(files_list) == sorted(
        [
            "logfile-a.txt",
            "logfile-b.txt",
            "test_input_1-outfile.txt",
            "test_input_10-outfile.txt",
            "test_input_2-outfile.txt",
            "test_input_3-outfile.txt",
            "test_input_4-outfile.txt",
            "test_input_5-outfile.txt",
            "test_input_6-outfile.txt",
            "test_input_7-outfile.txt",
            "test_input_8-outfile.txt",
            "test_input_9-outfile.txt",
        ]
    )
    for filename in files_list:
        if "outfile" in filename:
            with Path.open(Path(tmpdir.name, filename)) as outf:
                outfile_content = outf.readlines()
                assert len(outfile_content) == 1
                assert "Processed: " in outfile_content[0]

    for filename in files_list:
        if "logfile" in filename:
            with Path.open(Path(tmpdir.name, filename)) as logf:
                logfile_content = logf.readlines()
                assert len(logfile_content) == data_size


if __name__ == "__main__":
    test_multiprocessing_executor_process()
