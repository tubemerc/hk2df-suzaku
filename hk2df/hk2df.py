import os

import astropy.io.fits
import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))


class hk2df:
    def __init__(self, hk_dir_path: str):
        self.hk_dir_path = hk_dir_path

    def setup(
        self,
        start_datetime: str,
        end_datetime: str,
        filter_strings: list = None,
        include_matching_strings: bool = True,
        filter_units: list = None,
        include_matching_units: bool = True,
        calibrated_only: bool = True,
    ):
        # Set the fits files filter
        self.start_datetime = pd.to_datetime(start_datetime)
        self.end_datetime = pd.to_datetime(end_datetime)
        suzaku_table = pd.read_csv("suzaku_table.csv")
        _obs_start_list = pd.to_datetime(suzaku_table["observation_start_time"])
        _obs_end_list = pd.to_datetime(suzaku_table["observation_end_time"])
        start_idx = _obs_start_list[_obs_start_list <= self.start_datetime].index[-1]
        end_idx = _obs_end_list[_obs_end_list >= self.end_datetime].index[0]
        self.target_files = (
            suzaku_table[start_idx : end_idx + 1]["data_access_url"]
            .apply(self._url2fname)
            .to_list()
        )
        # Set the data filter
        with astropy.io.fits.open(
            "%s/%s" % (self.hk_dir_path, self.target_files[0])
        ) as f:
            _columns = ["index", "data_name", "unit"]
            self.data_filter = pd.DataFrame(columns=_columns)
            for i, hdu in enumerate(f):
                for header, value in hdu.header.items():
                    if "UNIT" in header:
                        if filter_units is not None:
                            if include_matching_units:
                                if not value in filter_units:
                                    continue
                            else:
                                if value in filter_units:
                                    continue
                        _name = hdu.header.get(header.replace("TUNIT", "TTYPE"))
                        if filter_strings is not None:
                            if include_matching_strings:
                                if not any(label in _name for label in filter_strings):
                                    continue
                            else:
                                if any(label in _name for label in filter_strings):
                                    continue
                        if calibrated_only and _name[-4:] != "_CAL":
                            continue
                        self.data_filter = pd.concat(
                            [
                                self.data_filter,
                                pd.DataFrame([[i, _name, value]], columns=_columns),
                            ],
                            ignore_index=True,
                            axis=0,
                        )

    def _url2fname(self, url):
        return "ae%s.hk" % url.rstrip("/").split("/")[-1]

    def to_dataframe(self):
        df = pd.DataFrame()
        idx_list = set(self.data_filter["index"].values)
        for i in idx_list:
            print("Processing index: %d/%d" % (i, len(idx_list)))
            data_names = self.data_filter[self.data_filter["index"] == i][
                "data_name"
            ].to_list()
            for j, filename in enumerate(self.target_files):
                with astropy.io.fits.open("%s/%s" % (self.hk_dir_path, filename)) as f:
                    _data = f[i].data
                    datetime = [
                        pd.to_datetime(x + y, format="%Y%m%d%H%M%S")
                        for (x, y) in zip(
                            [str(x) for x in _data.field("YYYYMMDD")],
                            [str(x).zfill(6) for x in _data.field("HHMMSS")],
                        )
                    ]
                    df_per_file = pd.DataFrame(index=datetime, columns=data_names)
                    for data_name in data_names:
                        df_per_file[data_name] = _data.field(data_name)
                    if j == 0:
                        df_per_index = df_per_file.copy()
                    else:
                        df_per_index = pd.concat(
                            [df_per_index, df_per_file.copy()], axis=0
                        )
            df_per_index.sort_index(inplace=True)
            df_per_index = df_per_index[~df_per_index.index.duplicated(keep="first")]
            df = pd.merge(
                df, df_per_index.copy(), how="outer", left_index=True, right_index=True
            )
        df.sort_index(inplace=True)
        df = df[~df.index.duplicated(keep="first")]
        df = df[self.start_datetime : self.end_datetime]
        return df
