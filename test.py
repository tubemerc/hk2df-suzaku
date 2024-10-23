from hk2df import hk2df

hk = hk2df.hk2df("D:\suzaku_data")
hk.setup(
    "2011-01-01",
    "2011-01-07",
    filter_strings=["DIST", "HCE"],
    include_matching_strings=True,
    filter_units=None,
    include_matching_units=False,
    calibrated_only=True,
)
print(hk.data_filter)
print(hk.target_files)
print(hk.to_dataframe())
