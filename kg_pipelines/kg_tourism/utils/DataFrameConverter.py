# https://stackoverflow.com/questions/37513355/converting-pandas-dataframe-into-spark-dataframe-error/56895546#56895546
# modified from parent gist by creating a dict type that contains df.dtypes AND type(pd.columnname)
# 
# Looks something like this.
#  {  'stringtypecolumn': {'dtype': 'object', 'actual': 'str'},
#     'act_num': {'dtype': 'int32', 'actual': 'numpy.int32'},
#     'text_dat': {'dtype': 'object', 'actual': 'list'},
#     'scene_description': {'dtype': 'object', 'actual': 'NoneType'},
#     'keywords': {'dtype': 'object', 'actual': 'list'}}
#

#Updated to be a class on 5 AUG 2019

from pyspark.shell import sqlContext
from pyspark.sql.types import StringType, StructField, ArrayType, IntegerType, FloatType, DateType, LongType, StructType

class DataFrameConverter:
    """A class that takes in a pandas data frame and converts it to a spark data frame..."""

    dtypeHeader = 'dtype'
    actualHeader = 'actual'

    def debug(self):
        print(f"dataframe type{type(self.df)}")
        print(f"dtypeHeader{self.dtypeHeader}")

    def get_pdf_column_meta(self, column_name_set):
        column_meta = {}
        for ch in column_name_set:
            column_meta.update({ch: {
                self.dtypeHeader: str(self.df[ch].dtypes),
                self.actualHeader: str(type(self.df[ch][0])).split("'")[1]
            }})
        return column_meta

    def equivalent_type(self, dtype, actual):
        if dtype == 'datetime64[ns]':
            return DateType()
        elif dtype == 'int64':
            return LongType()
        elif dtype == 'int32':
            return IntegerType()
        elif dtype == 'float64':
            return FloatType()
        elif dtype == "object" and actual == "list":
            return ArrayType(StringType())
        else:
            return StringType()

    def define_structure(self, column, tpe1, tpe2):
        try:
            typo = self.equivalent_type(tpe1, tpe2)
        except:
            typo = StringType()
            print("not ok match type, resorting to string")
        struct_field_return = StructField(column, typo)
        return struct_field_return

    def get_spark_df(self, df):
        self.df = df
        meta = self.get_pdf_column_meta(self.df.columns)
        struct_list = []
        for x in meta:
            # tpe = col_attr(meta, str(x))
            tpe = [str(meta.get(x).get(self.dtypeHeader)), str(meta.get(x).get(self.actualHeader))]
            struct_list.append(self.define_structure(x, tpe[0], tpe[1]))
        p_schema = StructType(struct_list)
        return sqlContext.createDataFrame(self.df, p_schema)
    
 
# To use
# load your pandas DF as pandasDF
#  
#from {path}.DataFrameConverter import DataFrameConverter as dfc
#convertedToSpark = dfc().get_spark_df(pandasDF)
#convertedToSpark.printSchema()
#check your [ArrayType[StringType]] columns.
