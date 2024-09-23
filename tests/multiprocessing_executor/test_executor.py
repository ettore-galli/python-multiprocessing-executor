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
    tmpdir: str,
    queue: Queue,
    feedback_writer: FeedbackWriter,
) -> None:
    for item in get_from_queue(queue):
        content = item.item
        outfile = Path(tmpdir, f"{content}-outfile.txt")
        with Path.open(outfile, "w", encoding="utf-8") as workf:
            processed_item = f"Processed: {item}"
            workf.write(processed_item)
            feedback_writer(processed_item)


def feedback_worker(logfile: Path, queue: Queue) -> None:
    with Path.open(logfile, "w", encoding="utf-8") as logf:
        for item in get_from_queue(queue):
            log_message = f"Feedback for: {item}\n"
            logf.write(log_message)


def test_multiprocessing_executor_process() -> None:
    tmpdir = TemporaryDirectory("multi-processing")

    data_size = 10

    def data_source() -> Generator[ExampleExecutorPayload, None, None]:
        for index in range(data_size):
            yield ExampleExecutorPayload(item=f"test_input_{index + 1}")

    logfile = Path(tmpdir.name, "logfile.txt")

    properties = MultiprocessingExecutorProperties(
        workers=3,
        processing_worker=partial(processing_worker, tmpdir.name),
        feedback_worker=partial(feedback_worker, logfile),
        task_source=data_source(),
    )

    executor = MultiprocessingExecutor(properties=properties)
    executor.perform()

    files_list = os.listdir(tmpdir.name)
    assert sorted(files_list) == sorted(
        [
            "logfile.txt",
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

    with Path.open(Path(tmpdir.name, "logfile.txt")) as logf:
        logfile_content = logf.readlines()
        assert len(logfile_content) == data_size


if __name__ == "__main__":
    test_multiprocessing_executor_process()
