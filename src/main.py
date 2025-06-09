import query as my_query
import salesfoce_transform as sf_transform
import salesforce_upload as sf_upload


def top_ten_banks():
    my_query.fdic_top_ten_banks_query()
    bank_data = sf_transform.sf_transform_fdic_banks()
    sf_upload.sf_upload(bank_data)

top_ten_banks()