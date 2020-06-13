#!/usr/bin/env python
import argparse
from xls_parser import XlsParser
SHEET_NAME = 'לוח 2 ייצור ברוטו בפועל '


def main():
    arg_parser = argparse.ArgumentParser(description='nzo-il/data-acquisition')
    arg_parser.add_argument('--input_file',
                            action='store',
                            type=str,
                            help='Input spreadsheet file',
                            required=True)
    arg_parser.add_argument('--sheet_name',
                            action='store',
                            type=str,
                            help='Spreadsheet name',
                            default=SHEET_NAME)
    arg_parser.add_argument('--output_file',
                            action='store',
                            type=str,
                            help='Output file name',
                            default='')
    args = arg_parser.parse_args()
    xls_parser = XlsParser(file_name=args.input_file,
                           sheet_name=args.sheet_name,
                           output_file=args.output_file)
    xls_parser.run()


if __name__ == "__main__":
    main()
