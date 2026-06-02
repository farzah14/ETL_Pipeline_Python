import pandas as pd
import sys
import logging
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
from unittest.mock import MagicMock, patch
from src.load import load_dimensions, load_fact

logger = logging.getLogger(__name__)
@patch("pandas.DataFrame.to_sql")

def test_load_dimensions_basic(mock_to_sql):
    engine = MagicMock()
    mock_conn = engine.begin.return_value.__enter__.return_value

    mock_conn.execute.return_value.fetchall.side_effect = [[], []]
    
    df = pd.DataFrame({
        "CustomerID": [1, 1, 2],
        "Country": ["USA", "USA", "UK"],
        "StockCode": ["65828E", "84987B", "90730C"],
        "Description": ["ProdA", "ProdA", "ProdB"]
    })

    with pd.option_context("mode.chained_assignment", None):
        load_dimensions(df, engine)
    
    assert mock_conn.execute.call_count==2
    assert mock_to_sql.call_count==2
    assert mock_to_sql.call_args_list[0][0][0] == "dim_customers"
    assert mock_to_sql.call_args_list[1][0][0] == "dim_products"

@patch("pandas.DataFrame.to_sql")
def test_loads_facts_basic(mock_to_sql):
    engine = MagicMock()
    mock_conn = engine.begin.return_value.__enter__.return_value

    mock_conn.execute.return_value.fetchall.side_effect = [[]]
    
    df = pd.DataFrame({
        "InvoiceNo": [1, 1, 2],
        "StockCode": ["65828E", "84987B", "90730C"],
        "Quantity": [1, 1, 2],
        "UnitPrice": [1, 1, 2],
        "TotalPrice": [1, 1, 2],
        "InvoiceDate": ["2022-01-01", "2022-01-01", "2022-01-01"],
        "CustomerID": [1, 1, 2],
        "Country": ["USA", "USA", "UK"]
    })

    with pd.option_context("mode.chained_assignment", None):
        load_fact(df, engine)
    
    assert mock_conn.execute.call_count==1
    assert mock_to_sql.call_count == 1
    assert mock_to_sql.call_args_list[0][0][0] == "fact_sales"

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    test_load_dimensions_basic()
    test_loads_facts_basic()
    print("Test passed successfully!")