if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Starting count of the Data : ", len(data))


    # Remove data with 0 passenger Count / 0 Trip Distance 
    data = data[(data["trip_distance"] > 0) & (data["passenger_count"] > 0 )]

    print("len",len(data[(data["trip_distance"] > 0) & (data["passenger_count"] > 0 )]))
    # Converting Datatime to date 
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Camel_case to Snake_case
    df_col_cp = data.columns
    df_col_fir = [x.replace('ID','_id') for x in  df_col_cp]
    df_col_fir = [x.replace('Location','_location') for x in df_col_fir]
    df_col_fir = [x.lower() for x in df_col_fir]

    data.columns = df_col_fir

    return data


@test
def test_output(data, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert any(data.columns == "vendor_id"), 'The output is undefined'
    valid_ids = [1,2]
    assert data['vendor_id'].isin(valid_ids).all() , 'It is avaliable'
    assert (data['passenger_count'] > 0).all(), 'Passenger count is greater than 0'
    assert (data['trip_distance'] > 0).all(), "trip_distance is greater than 0" 
