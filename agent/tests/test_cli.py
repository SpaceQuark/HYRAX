import argparse

import pytest

from hyrax import cli


@pytest.mark.parametrize("text, expected", [("24G", 24576), ("500M", 500), ("1.5g", 1536), ("8000", 8000)])
def test_memory_sizes(text, expected):
    assert cli.megabytes(text) == expected


def test_a_bad_memory_size_is_an_argument_error():
    with pytest.raises(argparse.ArgumentTypeError):
        cli.megabytes("lots")
