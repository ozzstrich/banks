import query as my_query
import salesfoce_transform as sf_transform


def top_ten_banks():
    my_query.fdic_top_ten_banks_query()
    sf_transform.sf_transform_fdic_banks()


top_ten_banks()