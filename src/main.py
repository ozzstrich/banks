import query as my_query
import salesforce_transform as sf_transform
import salesforce_upload as sf_upload


def top_ten_banks():
    my_query.fdic_top_ten_banks_query()
    bank_data = sf_transform.sf_transform_fdic_banks()
    sf_upload.sf_upsert("Account", bank_data, "salesforce_id")

top_ten_banks()

# gitpushtest