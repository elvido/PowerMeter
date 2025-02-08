import time
from typing import Optional, Tuple

from rich.console import Console, JustifyMethod, RenderableType, Theme
from rich.progress import (
    FileSizeColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)
from rich.style import StyleType
from rich.table import Column
from rich.text import Text


class TargetColumn(ProgressColumn):
    """Custom column to show the estimated time remaining with a defined style."""

    def __init__(
        self,
        text_format: str,
        style: Optional["StyleType"] = "progress.target.name",
        justify: Optional["JustifyMethod"] = "left",
        pulse: Optional["str"] = None,
        pulse_style: Optional["StyleType"] = "progress.target.pulse",
        borders: Optional[Tuple["str", "str"]] = None,
        markup: bool = True,
        table_column: Optional[Column] = None,
        speed: float = 1.0,
    ) -> None:
        self.text_format = text_format
        self.justify: JustifyMethod = justify
        self.style = style
        self.markup = markup
        if pulse is not None:
            self.pulse = pulse
        else:
            self.pulse = ">"
        self.pulse_style = pulse_style
        self.speed = speed
        self.start_time = None
        if borders:
            _front, _rear = borders
        else:
            _front, _rear = ("[", "]")
        if self.markup:
            self.front = Text.from_markup(_front, style=self.style)
            self.rear = Text.from_markup(_rear, style=self.style)
        else:
            self.front = Text(_front, self.style)
            self.rear = Text(_rear, self.style)
        super().__init__(table_column=table_column or Column(no_wrap=True))

    def render(self, task: "Task") -> "RenderableType":
        if self.start_time is None:
            self.start_time = task.get_time()
        _text_str = self.text_format.format(task=task)
        if not _text_str or len(_text_str) < 1:
            _text_str = "<unspecified>"
        _pulse_str = self.pulse.format(task=task)
        if self.markup:
            _text = Text.from_markup(
                _text_str, style=self.style, justify=self.justify)
            _pulse = Text.from_markup(_pulse_str, style=self.pulse_style)
        else:
            _text = Text(_text_str, style=self.style, justify=self.justify)
            _pulse = Text.from_markup(_pulse_str, style=self.pulse_style)

        if not task.finished:
            n = len(_pulse) - 1
            _pos = int(
                ((task.get_time() - self.start_time)
                 * self.speed) / (len(_text) / 40.0)
            ) % (len(_text) - max(n, 0))
            return Text.assemble(
                self.front,
                _text[:_pos],
                _pulse if n >= 0 else _text[_pos],
                _text[_pos + max(n, 0) + 1:],
                self.rear,
            )
        return _text


def main():
    console = Console(
        theme=Theme(
            {
                "progress.target.name": "magenta",
                "progress.target.pulse": "green bold",
                "progress.description": "red",
                "progress.spinner": "black",
                "progress.percentage": "yellow",
            }
        )
    )

    with Progress(
        TextColumn("{task.description}", style="progress.description"),
        SpinnerColumn(spinner_name="bouncingBar"),
        "|",
        TimeElapsedColumn(),
        "|",
        TransferSpeedColumn(),
        "|",
        FileSizeColumn(),
        "|",
        TargetColumn("{task.fields[target]}", pulse="{task.fields[pulse]}"),
        "|",
        TextColumn("{task.fields[message]}", style="progress.description"),
        console=console,
    ) as progress:
        task = progress.add_task(
            "Processing",
            total=None,
            target="consumption_data.csv",
            message="[blue]retrieving data...[/blue]",
            pulse=">"
        )

        progress.start_task(task)
        for i in range(100):
            time.sleep(0.1)  # Simulate work being done
            progress.advance(task, 5678)
        progress.update(
            task,
            description="Finished",
            total=progress.tasks[task].completed,
            refresh=True,
            message="data successfully persisted",
        )

    console.print("[green]Process complete![/green]")


if __name__ == "__main__":
    main()
