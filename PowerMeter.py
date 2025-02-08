#   ______                     ___  ___     _
#   | ___ \                    |  \/  |    | |
#   | |_/ /____      _____ _ __| .  . | ___| |_ ___ _ __
#   |  __/ _ \ \ /\ / / _ \ '__| |\/| |/ _ \ __/ _ \ '__|
#   | | | (_) \ V  V /  __/ |  | |  | |  __/ ||  __/ |
#   \_|  \___/ \_/\_/ \___|_|  \_|  |_/\___|\__\___|_|
#
# List of open issues and missing features:
#
# TODO: Implement click() command line processing.
# TODO: Add nice console interface via rich(). Add Ascii-Art and progress indicator
# TODO: Migrate data processing from dbeaver to python and duckdb
# TODO: Finalize download indicator
# TODO: Zip data files into one archive
# TODO: Add config file for various options / check click python package
# TODO: Add data folder

import os
import shutil
import signal
import sys
from datetime import datetime
from threading import Event
from urllib.parse import urljoin

import duckdb
import pandas as pd
import requests
from rich.console import Console, Theme
from rich.progress import (
    FileSizeColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)

import TargetColumn

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


def handle_sigint(signal, frame):
    console.print("[red]Download canceled by user (SIGINT received)[/red]")
    sys.exit(0)


# Register the SIGINT signal handler
signal.signal(signal.SIGINT, handle_sigint)
# Define a DataChannel class


class DataChannel:
    def __init__(self, channel, path, filename):
        self.channel = channel
        self.path = path
        self.filename = filename


# Create a list of data channel objects
data_channels = [
    #     DataChannel('L1-Energy',  'emeter/0/em_data.csv', 'L1-em_data.csv'),
    #     DataChannel('L2-Energy',  'emeter/1/em_data.csv', 'L2-em_data.csv'),
    #     DataChannel('L3-Energy',  'emeter/2/em_data.csv', 'L3-em_data.csv'),
    #     DataChannel('L1-Voltage', 'emeter/0/vm_data.csv', 'L1-vm_data.csv'),
    #     DataChannel('L2-Voltage', 'emeter/1/vm_data.csv', 'L2-vm_data.csv'),
    # DataChannel('L3-Voltage', 'emeter/2/vm_data.csv', 'L3-vm_data.csv')
]

shelly_url = "http://powermeter.fritz.box/"

console = Console()

cancel_event = Event()


def handle_sigint(signum, frame):
    cancel_event.set()


signal.signal(signal.SIGINT, handle_sigint)

# Function to download a CSV file from a given URL


def download_data(channel, url, filename):
    try:
        # Stream the content to avoid loading it all into memory
        response = requests.get(url, stream=True)

        # Check for HTTP errors
        if response.status_code != 200:
            console.print(
                f"[red]Error: Received status code {response.status_code} for URL: {url}[/red]"
            )
            return

        response.raise_for_status()

        # Get the total file size (if available)
        total_size = int(response.headers.get("content-length", 0))

        # Check if the file already exists and create a backup
        if os.path.exists(filename):
            backup_filename = filename + ".bak"
            shutil.move(filename, backup_filename)
            console.print(
                f"[blue]Renamed existing file to {backup_filename}[/blue]")

        # Use temp file
        tempfile = filename + ".temp"

        # Open the temp file for writing
        with open(tempfile, "wb") as file:
            # Use rich's Progress to display the download progress
            with Progress(
                TextColumn(f"{channel}"),
                SpinnerColumn(spinner_name="bouncingBar", style="yellow"),
                "|",
                TimeElapsedColumn(),
                "|",
                TransferSpeedColumn(),
                "|",
                FileSizeColumn(),
                "|",
                TargetColumn("{task.fields[target]}",
                             pulse="{task.fields[pulse]}"),
                "|",
                TextColumn("{task.fields[message]}",
                           style="progress.description"),
                console=console,
            ) as progress:
                task = progress.add_task(
                    "Downloading",
                    start=False,
                    total=total_size if total_size > 0 else None,
                    target="Target[blue]FileName[/blue]",
                    message="Hallo [blue]Ralf[/blue]",
                    pulse="[red]|=|",
                )
                progress.start_task(task)
                for data in response.iter_content(chunk_size=1024):
                    # and 'Another file transfer is in progress' in data:
                    if len(data) == 37:
                        console.print(
                            "[red]Error: Download canceled due another active download.[/red]"
                        )
                        return
                    if cancel_event.is_set():
                        raise KeyboardInterrupt
                    file.write(data)
                    progress.update(task, advance=len(data))
                progress.update(
                    task,
                    description="Finished",
                    total=progress.tasks[task].completed,
                    refresh=True,
                    message="we are done",
                )
                console.print(" ---> done")

        # Rename temp file to final file after success
        shutil.move(tempfile, filename)
        console.print(f"[green]Downloaded {filename}[/green]")

    except requests.exceptions.RequestException as e:
        console.print(f"[red]Error downloading file: {e}[/red]")
    except KeyboardInterrupt:
        console.print("[red]Download interrupted by user[/red]")
    except Exception as e:
        console.print(f"[red]An unexpected error occurred: {e}[/red]")
        if os.path.exists(tempfile):
            os.remove(tempfile)


def main():
    # Connect to DuckDB
    conn = duckdb.connect()

    # Create a Pandas DataFrame
    data = {"a": [1, 2, 3, 7], "b": [4, 5, 6, 8]}
    df = pd.DataFrame(data)

    # Write the DataFrame to DuckDB
    conn.register("my_table", df)

    # Run a query and fetch results as a DataFrame
    result_df = conn.sql("SELECT * FROM my_table").df()
    console.print(result_df)

    # Close the connection
    conn.close()

    # Get the current date
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Loop through the list of URLs and download each file
    for data_channel in data_channels:
        download_data(
            data_channel.channel,
            urljoin(shelly_url, data_channel.path),
            data_channel.filename,
        )


if __name__ == "__main__":
    main()
