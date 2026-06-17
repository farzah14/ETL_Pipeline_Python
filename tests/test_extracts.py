import logging
import pandas as pd
import sys
import logging
from pathlib import Path
from src.extract import extract_get_data

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

