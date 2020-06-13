from xls_parser import XlsParser


def test_init():
    file_name = "inputs/2019/12_2019.xlsx"
    sheet_name = 'לוח 2 ייצור ברוטו בפועל '
    parser = XlsParser(file_name=file_name,
                       sheet_name=sheet_name,
                       output_file='')
    assert parser
