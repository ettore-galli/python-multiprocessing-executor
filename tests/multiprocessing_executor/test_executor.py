import os
from collections.abc import Generator
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory

from multiprocessing_executor.executor.executor import (
    FeedbackWriter,
    MultiprocessingExecutor,
    MultiprocessingExecutorProperties,
)


def item_processor(tempdir: str, feedback_writer: FeedbackWriter, item: str) -> None:
    processed_item = "Processed: {item}"
    outfile = Path(tempdir, item)
    with Path.open(outfile, "w", encoding="utf-8") as workf:
        workf.write(processed_item)
        feedback_writer(f"processed {item}")


def item_feedback_processor(tempdir: str, *args: tuple, **_: dict) -> None:
    item_feedback = "Feedback of: {item}"
    outfile = Path(tempdir, f"{args[0]}_feedback")
    with Path.open(outfile, "w", encoding="utf-8") as feedbackf:
        feedbackf.write(item_feedback)


def test_multiprocessing_executor_process() -> None:
    tmpdir = TemporaryDirectory("multi-processing")

    def data_source() -> Generator[str, None, None]:
        for index in range(10):
            yield f"test_input_{index + 1}"

    properties = MultiprocessingExecutorProperties(
        workers=3,
        item_processor=partial(item_processor, tmpdir.name),
        item_feedback_processor=partial(item_feedback_processor, tmpdir.name),
        task_source=data_source(),
    )

    executor = MultiprocessingExecutor(properties=properties)
    executor.perform()

    files_list = os.listdir(tmpdir.name)
    assert sorted(files_list) == sorted(
        [
            "processed test_input_10_feedback",
            "processed test_input_1_feedback",
            "processed test_input_2_feedback",
            "processed test_input_3_feedback",
            "processed test_input_4_feedback",
            "processed test_input_5_feedback",
            "processed test_input_6_feedback",
            "processed test_input_7_feedback",
            "processed test_input_8_feedback",
            "processed test_input_9_feedback",
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
