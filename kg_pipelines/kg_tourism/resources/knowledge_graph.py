import kglab
from kglab.util import get_gpu_count
from typing import List

import pandas as pd

if get_gpu_count() > 0:
    import cudf  # type: ignore  # pylint: disable=E0401

class KnowledgeGraph(kglab.KnowledgeGraph):
    def load_from_df(self, df: pd.DataFrame) -> "KnowledgeGraph":
        """Populate a knowledge graph from a Pandas dataframe."""
        
        df.apply(
            lambda row: self._g.parse(data="{} {} {} .".format(row[0], row[1], row[2]), format="ttl"),
            axis=1,
        )

        return self
    def export_df(self) -> pd.DataFrame:
        rows_list: List[dict] = [
            {
                self._PARQUET_COL_NAMES[0]: s.n3(),
                self._PARQUET_COL_NAMES[1]: p.n3(),
                self._PARQUET_COL_NAMES[2]: o.n3(),
            }
            for s, p, o in self._g
        ]

        if self.use_gpus:
            df = cudf.DataFrame(rows_list, columns=self._PARQUET_COL_NAMES)
        else:
            df = pd.DataFrame(rows_list, columns=self._PARQUET_COL_NAMES)
        return df