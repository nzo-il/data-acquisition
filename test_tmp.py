import pytest
from xls_parser import XlsParser

def test_init():
    filename = "inputs/2019/12_2019.xlsx"
    sheetname = 'לוח 2 ייצור ברוטו בפועל '
    parser = XlsParser(filename=filename, sheetname=sheetname)
    assert parser
