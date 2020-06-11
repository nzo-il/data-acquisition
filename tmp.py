import pandas
import re
import json
import os

class XlsParser:
    def __init__(self, filename, sheetname):
        self.input_filename = filename
        self.input_sheetname = sheetname
        self.data = pandas.ExcelFile(filename)
        self.s = self.data.parse(sheetname)
        self.limit = 50
        self.anchor = None
        self.col_len = len(self.s.columns)
        self.row_len = len(self.s.values)
        self.electric_data = {}
        self.electric_data_by_type = {}
        self.timestamps_len = None
        self.not_found = set()
        with open("inputs/mapping/mapping.json") as fh:
            self.mapping = json.loads(fh.read())

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

    def aggregate_by_type(self):
        for name, electric_type in self.mapping.items():
            if electric_type not in self.electric_data_by_type:
                self.electric_data_by_type[electric_type] = [0] * len(self.electric_data['timestamps'])

            if name not in self.electric_data:
                self.not_found.add(name)
                continue

            for count, item in enumerate(self.electric_data[name]):
                if str(item) == "nan":
                    continue
                self.electric_data_by_type[electric_type][count] += item

        print("Didn't find %s" % self.not_found)


    def write_output(self):
        output_filename = re.sub(r"^inputs/", r"outputs/", self.input_filename)
        output_filename = re.sub(r"\.xlsx$", r".csv", output_filename)
        dir_name = os.path.dirname(output_filename)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        fh = open(output_filename, "w")
        row = "timestamp"
        for electric_type in parser.electric_data_by_type:
            row += ", %s" % electric_type
        fh.write("%s\n" % row)

        for count, timestamp in enumerate(parser.electric_data['timestamps']):
            row = str(timestamp)
            for electric_type in parser.electric_data_by_type:
                row += ", %s" % parser.electric_data_by_type[electric_type][count]
            fh.write("%s\n" % row)
        fh.close()


filename = "inputs/2019/12_2019.xlsx"
sheetname = 'לוח 2 ייצור ברוטו בפועל '

parser = XlsParser(filename=filename, sheetname=sheetname)
parser.get_anchor()
parser.get_electric_data()
parser.aggregate_by_type()
parser.write_output()





