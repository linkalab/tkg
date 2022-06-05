import pandas as pd

def split_old_new(context, asset_name, df, old_df, id_column, should_update):
    if should_update and old_df is None:
        context.log.warn("We should update asset %s BUT it does not exist in the data lake... we create it from scratch" % asset_name)
        old_df = pd.DataFrame(columns=df.columns) ## create an empty data frame
    elif should_update:
        old_rows = df[id_column].isin(old_df[id_column])
        df = df[~old_rows] ## do not enrich rows already enriched 
    elif not should_update:
        old_df = pd.DataFrame(columns=df.columns) ## force original data frame to be empty
    return df, old_df