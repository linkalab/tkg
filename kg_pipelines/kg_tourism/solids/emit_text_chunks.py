from dagster import solid, DynamicOutputDefinition, DynamicOutput
from dagster.config.field import Field
from pandas import DataFrame

@solid(
    config_schema={"batch_size": Field(int, is_required=False)},
    output_defs=[
        DynamicOutputDefinition(
            DataFrame,
            description="A dynamic set of pandas dataframes with text and id",
            io_manager_key="temporary_io_manager"
        )
    ],
)
def dynamic_text_chunks(context, text_df: DataFrame):
    """
    For the input DataFrame produce multiple chunks as DataFrame
    """
    # text_column = "comment"
    # id_column = "review_id"
    # df = text_df[[id_column, text_column]] #[0:4000]

    ## We take the first two columns in order
    ## The first must be the id column
    ## The second must be the text column
    df = text_df.iloc[:, 0:2] #[0:4000]

    df = df.dropna()
    df.columns=["id", "text"]
    end_row = df.shape[0]
    start_row = 0
    batch_size = context.solid_config.get("batch_size")
    if batch_size is not None and batch_size > 1:
        start = start_row
        while start < end_row:
            end = start + batch_size
            end = end if end <= end_row else end_row
            yield DynamicOutput(df[start:end], mapping_key=f"{start}_{end}")
            start += batch_size

    else:
        yield DynamicOutput(
            df,
            mapping_key=f"{start_row}_{end_row}",
            #metadata_entries=metadata_entries,
        )