#!/usr/bin/env python3
"""Pretty-print structured MIT 6.5840 Raft logs by peer."""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, TextIO


LOG_PATTERN = re.compile(
    r"^\[(?P<time_ms>\d+)ms\]\s+"
    r"(?P<topic>[A-Z][A-Z0-9_]*)\s+"
    r"PEER=(?P<peer>\d+)\s+"
    r"EVENT=(?P<event>[A-Z][A-Z0-9_]*)"
    r"(?P<fields>(?:\s+.*)?)$"
)
TEST_START_PATTERN = re.compile(r"^=== RUN\s+")
TEST_END_PATTERN = re.compile(r"^--- (?:PASS|FAIL):\s+")

RESET = "\033[0m"
TOPIC_COLORS = {
    "VOTE": "\033[96m",
    "TIME": "\033[90m",
    "APPEND": "\033[94m",
    "STEP_DOWN": "\033[93m",
    "AGREEMENT": "\033[95m",
    "INIT": "\033[92m",
}

DEFAULT_OUTPUT_WIDTH = 160
MIN_COLUMN_WIDTH = 12
COLUMN_SEPARATOR = " │ "
TIME_HEADER = "Time"


@dataclass(frozen=True)
class LogEvent:
    time_ms: int
    topic: str
    peer: int
    event: str
    fields: str

    def display_text(self) -> str:
        suffix = f" {self.fields}" if self.fields else ""
        return f"{self.topic} {self.event}{suffix}"

    def display_time(self) -> str:
        return f"[{self.time_ms}ms]"


@dataclass(frozen=True)
class SegmentLine:
    raw: str
    event: LogEvent | None


def parse_topics(value: str) -> set[str]:
    topics = {topic.strip().upper() for topic in value.split(",") if topic.strip()}
    if not topics:
        raise argparse.ArgumentTypeError("topic list must not be empty")
    return topics


def parse_event(line: str) -> LogEvent | None:
    match = LOG_PATTERN.fullmatch(line)
    if match is None:
        return None

    return LogEvent(
        time_ms=int(match.group("time_ms")),
        topic=match.group("topic"),
        peer=int(match.group("peer")),
        event=match.group("event"),
        fields=match.group("fields").strip(),
    )


def read_segments(lines: Iterable[str]) -> Iterable[list[SegmentLine]]:
    segment: list[SegmentLine] = []

    for input_line in lines:
        line = input_line.rstrip("\n")

        if TEST_START_PATTERN.match(line) and segment:
            yield segment
            segment = []

        segment.append(SegmentLine(raw=line, event=parse_event(line)))

        if TEST_END_PATTERN.match(line):
            yield segment
            segment = []

    if segment:
        yield segment


def output_width(out: TextIO) -> int:
    if out.isatty():
        return shutil.get_terminal_size(fallback=(DEFAULT_OUTPUT_WIDTH, 24)).columns
    return DEFAULT_OUTPUT_WIDTH


def calculate_column_width(
    total_width: int,
    peer_count: int,
    time_width: int,
) -> int:
    separators_width = len(COLUMN_SEPARATOR) * peer_count
    available_width = total_width - time_width - separators_width
    return max(MIN_COLUMN_WIDTH, available_width // peer_count)


def topic_is_visible(
    topic: str,
    just: set[str] | None,
    ignore: set[str],
) -> bool:
    if just is not None and topic not in just:
        return False
    return topic not in ignore


def colorize(text: str, topic: str, use_color: bool) -> str:
    color = TOPIC_COLORS.get(topic)
    if not use_color or color is None or not text:
        return text
    return f"{color}{text}{RESET}"


def print_full_width(line: str, out: TextIO) -> None:
    print(line, file=out)


def print_header(
    peer_count: int,
    column_width: int,
    time_width: int,
    out: TextIO,
) -> None:
    labels = []
    rules = []
    for peer in range(peer_count):
        label = f"Peer {peer}"
        labels.append(label.center(column_width))
        rules.append("─" * column_width)

    print(
        TIME_HEADER.ljust(time_width)
        + COLUMN_SEPARATOR
        + COLUMN_SEPARATOR.join(labels).rstrip(),
        file=out,
    )
    print(
        "─" * time_width + COLUMN_SEPARATOR + COLUMN_SEPARATOR.join(rules).rstrip(),
        file=out,
    )


def print_event(
    event: LogEvent,
    peer_count: int,
    column_width: int,
    time_width: int,
    use_color: bool,
    out: TextIO,
) -> None:
    wrapped = textwrap.wrap(
        event.display_text(),
        width=column_width,
        break_long_words=True,
        break_on_hyphens=False,
        replace_whitespace=False,
        drop_whitespace=True,
    ) or [""]

    for line_number, part in enumerate(wrapped):
        cells: list[str] = []
        for peer in range(peer_count):
            plain_text = part if peer == event.peer else ""
            styled_text = colorize(plain_text, event.topic, use_color)
            cells.append(styled_text + " " * (column_width - len(plain_text)))
        displayed_time = event.display_time() if line_number == 0 else ""
        print(
            displayed_time.ljust(time_width)
            + COLUMN_SEPARATOR
            + COLUMN_SEPARATOR.join(cells).rstrip(),
            file=out,
        )


def render_segment(
    segment: list[SegmentLine],
    just: set[str] | None,
    ignore: set[str],
    use_color: bool,
    width: int,
    out: TextIO,
) -> None:
    events = [line.event for line in segment if line.event is not None]
    if not events:
        for line in segment:
            print_full_width(line.raw, out)
        return

    peer_count = max(event.peer for event in events) + 1
    time_width = max(
        len(TIME_HEADER), max(len(event.display_time()) for event in events)
    )
    column_width = calculate_column_width(width, peer_count, time_width)
    header_printed = False

    for line in segment:
        if line.event is None:
            print_full_width(line.raw, out)
            continue

        if not topic_is_visible(line.event.topic, just, ignore):
            continue

        if not header_printed:
            print_header(peer_count, column_width, time_width, out)
            header_printed = True

        print_event(
            event=line.event,
            peer_count=peer_count,
            column_width=column_width,
            time_width=time_width,
            use_color=use_color,
            out=out,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Colorize and arrange structured Raft logs into peer columns."
    )
    parser.add_argument(
        "log_file",
        nargs="?",
        type=Path,
        help="log file to read; omit to read from stdin",
    )
    parser.add_argument(
        "-j",
        "--just",
        type=parse_topics,
        help="comma-separated topics to include",
    )
    parser.add_argument(
        "-i",
        "--ignore",
        type=parse_topics,
        default=set(),
        help="comma-separated topics to exclude",
    )

    color_group = parser.add_mutually_exclusive_group()
    color_group.add_argument(
        "--color",
        dest="color",
        action="store_true",
        help="force ANSI color output",
    )
    color_group.add_argument(
        "--no-color",
        dest="color",
        action="store_false",
        help="disable ANSI color output",
    )
    parser.set_defaults(color=None)
    return parser


def open_input(path: Path | None) -> tuple[TextIO, bool]:
    if path is None:
        return sys.stdin, False
    return path.open("r", encoding="utf-8", errors="replace"), True


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    use_color = args.color if args.color is not None else True
    width = output_width(sys.stdout)

    try:
        source, should_close = open_input(args.log_file)
    except OSError as error:
        parser.error(str(error))

    try:
        for segment in read_segments(source):
            render_segment(
                segment=segment,
                just=args.just,
                ignore=args.ignore,
                use_color=use_color,
                width=width,
                out=sys.stdout,
            )
    except BrokenPipeError:
        return 0
    finally:
        if should_close:
            source.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
