from abc import ABC
from typing import Literal

import pandas as pd
from pandera import DataFrameSchema, Column, Check


class DataframeModel(ABC):
    schema: DataFrameSchema
    name: str
    type: Literal["csv", "xlsx"] = "csv"

    def load(self) -> pd.DataFrame:
        # cache management can be done here

        if self.type == 'csv':
            return self.schema.validate(
                pd.read_csv(f"data/{self.name}.{self.type}")
            )
        elif self.type == 'xlsx':
            return self.schema.validate(
                pd.read_excel(f"data/{self.name}.{self.type}")
            )
        raise ValueError(f"Unknown type {self.type}")

    def save(self, df: pd.DataFrame) -> None:
        if self.type == 'csv':
            self.schema.validate(df).to_csv(f"data/{self.name}.{self.type}")
            return
        elif self.type == 'xlsx':
            self.schema.validate(df).to_excel(f"data/{self.name}.{self.type}")
            return
        raise ValueError(f"Unknown type {self.type}")


class InputDataModel(DataframeModel):
    schema = DataFrameSchema(
        {
            "contractId": Column(int, report_duplicates="all", nullable=False),
            "engagement": Column(float, checks=[Check.greater_than_or_equal_to(0)], nullable=False, coerce=True),
            "categorie": Column(str, nullable=False, checks=[Check.isin(["A", "B", "C"])]),
        }
    )
    name = "input_data"
    type = "csv"


class OutputDataModel(DataframeModel):
    schema = InputDataModel.schema.add_columns({
        "engagement_doubled": Column(float, checks=[Check.greater_than_or_equal_to(0)], nullable=False)
    })
    name = "output_data"
    type = "xlsx"
