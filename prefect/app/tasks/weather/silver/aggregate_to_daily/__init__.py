"""Tasks for aggregating weather data to daily level."""

from prefect import task
from prefect.tasks import task_input_hash
from config.settings import settings

