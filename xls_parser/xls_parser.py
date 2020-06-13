import pandas
import time
import re
import json
import os

from typing import List
from typing import Dict
from typing import Set


def print_time(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        ret = func(*args, **kwargs)
        end = time.time()
        if args[0].verbose_mode:
            print("method '%s' took %.3f[sec] to complete"
                  % (func.__name__, end - start))
        return ret
    return wrapper


class XlsParser:
    @print_time
    def __init__(self, file_name: str,
                 sheet_name: str,
                 output_file: str) -> None:
        self.file_name = file_name
        self.sheet_name = sheet_name
        if output_file == '':
            output_file_name = re.sub(r"^inputs/", r"outputs/", self.file_name)
            output_file_name = re.sub(r"\.xlsx$", r".csv", output_file_name)
        else:
            output_file_name = self.output_file
        self.output_file = output_file_name
        self.verbose_mode = True
        self.data = None
        self.s = None
        self.limit = 50
        self.col_len = None
        self.row_len = None
        self.anchor = None
        self.electric_data: Dict[str, List[float]] = {}
        self.electric_data_by_type: Dict[str, List[float]] = {}
        self.timestamps_len = None
        self.mapping_not_found_in_electric_data: Set[str] = set()
        self.electric_data_not_found_in_mapping: Set[str] = set()
        with open("inputs/mapping/mapping.json") as fh:
            self.mapping = json.loads(fh.read())

    @print_time
    def read_excel_file(self):
        if self.data is not None:
            return
        self.data = pandas.ExcelFile(self.file_name)
        self.s = self.data.parse(self.sheet_name)
        self.col_len = len(self.s.columns)
        self.row_len = len(self.s.values)

    @print_time
    def get_anchor(self):
        for col in range(0, self.col_len):
            if col > self.limit:
                break
            try:
                self.s.iloc[0, col]
            except IndexError:
                break
            for row in range(0, self.row_len):
                if row > self.limit:
                    break
                try:
                    cell = self.s.iloc[row, col]
                    if re.match(r".*Unit\s*Name.*", str(cell)):
                        self.anchor = [row, col]
                        return
                except IndexError:
                    break

    @print_time
    def get_electric_data(self):
        column_name = "timestamps"
        self.electric_data[column_name] = []
        for row in range(self.anchor[0] + 1, self.row_len):
            timestamp = self.s.iloc[row, self.anchor[1]]
            if str(timestamp) == "nan":
                self.timestamps_len = len(self.electric_data[column_name])
                break
            self.electric_data[column_name].append(timestamp)

        for col in range(self.anchor[1] + 1, self.col_len):
            column_name = self.s.iloc[self.anchor[0], col]
            if str(column_name) == "nan":
                continue
            self.electric_data[column_name] = []
            start_row = self.anchor[0] + 1
            for row in range(start_row, start_row + self.timestamps_len):
                self.electric_data[column_name].append(self.s.iloc[row, col])

    @print_time
    def populate_electric_data_not_found_in_mapping(self):
        for name in self.electric_data:
            if name not in self.mapping:
                self.electric_data_not_found_in_mapping.add(name)

    @print_time
    def populate_mapping_not_found_in_electric_data(self):
        for name in self.mapping:
            if name not in self.electric_data:
                self.mapping_not_found_in_electric_data.add(name)

    @print_time
    def aggregate_by_type(self):
        for name, electric_type in self.mapping.items():
            if electric_type not in self.electric_data_by_type:
                self.electric_data_by_type[electric_type] \
                    = [0] * len(self.electric_data['timestamps'])

            for count, item in enumerate(self.electric_data.get(name, [])):
                if str(item) == "nan":
                    continue
                self.electric_data_by_type[electric_type][count] += item

    @print_time
    def write_output(self):
        dir_name = os.path.dirname(self.output_file)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        fh = open(self.output_file, "w")
        row = "timestamp"
        for electric_type in self.electric_data_by_type:
            row += ", %s" % electric_type
        fh.write("%s\n" % row)

        for count, timestamp in enumerate(self.electric_data['timestamps']):
            row = str(timestamp)
            for electric_type in self.electric_data_by_type:
                row += ", %s" % \
                       self.electric_data_by_type[electric_type][count]
            fh.write("%s\n" % row)
        fh.close()

        fh = open(re.sub(r"\.csv$", "_hour.csv", self.output_file), "w")
        row = "timestamp"
        for electric_type in self.electric_data_by_type:
            row += ", %s" % electric_type
        fh.write("%s\n" % row)

        for idx in range(0, len(self.electric_data['timestamps']), 2):
            row = str(self.electric_data['timestamps'][idx])
            for electric_type in self.electric_data_by_type:
                row += ", %s" % \
                       (self.electric_data_by_type[electric_type][idx] +
                        self.electric_data_by_type[electric_type][idx + 1])
            fh.write("%s\n" % row)
        fh.close()

        fh = open(re.sub(r"\.csv", ".report", self.output_file), "w")
        fh.write("mapping_not_found_in_electric_data:\n")
        fh.write("#" * 100 + "\n")
        for name in self.mapping_not_found_in_electric_data:
            fh.write("%s\n" % name)
        fh.write("#" * 100 + "\n")
        fh.write("electric_data_not_found_in_mapping:\n")
        fh.write("#" * 100 + "\n")
        for name in self.electric_data_not_found_in_mapping:
            fh.write("%s\n" % name)
        fh.close()

    @print_time
    def run(self):
        self.read_excel_file()
        self.get_anchor()
        self.get_electric_data()
        self.populate_electric_data_not_found_in_mapping()
        self.populate_mapping_not_found_in_electric_data()
        self.aggregate_by_type()
        self.write_output()
