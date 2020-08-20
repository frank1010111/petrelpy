#convert_production_petrel.py
# A script for converting IHS production data into a format readable by
# Petrel in the .vol format
# Copyright 2018 Frank Male


#A .vol file is supposed to look like this:
"""
*FIELD
*DAILY
*DAY *MONTH *YEAR *OIL *WATER *GAS *WINJ

*NAME Producer_1
01 01 1970  108.80  0.15  162.27  0
01 01 1971  197.05  0.29  290.15  0
01 01 1972  250.57  0.39  363.80  0
01 01 1973  334.36  0.56  477.98  0
01 01 1974  227.94  0.40  323.04  0

*NAME Injector_1
01 01 1970  0  0  0  1000
01 01 1971  0  0  0  1120
"""

import pandas as pd
import numpy as np
import argparse
import os
import sys

def read_data(infile,yearly=False):
    "Get raw data from infile (even if infile is several files)"
    # read in data and group by well
    if yearly:
        sheetname='Annual Production'
        cols = ['Annual Liquid','Annual Water','Annual Gas']
    else:
        sheetname='Monthly Production'
        cols = ['Liquid','Water','Gas']


    def readfile(fname):
        try:
            Raw = pd.read_csv(fname,converters={'Year':str,'API':str})
        except:
            Raw = pd.read_excel(fname,sheet_name=sheetname,converters={'Year':str,'API':str})
        return Raw

    if type(infile) in (list,tuple):
        sheets = [readfile(fname) for fname in infile]
        Raw = pd.concat(sheets)
    else:
        Raw = readfile(infile)

    if yearly:
        Raw['Date']=pd.to_datetime(Raw.Year,format='%Y')
    else:
        Raw['Date']=pd.to_datetime(Raw.Month+Raw.Year,format='%b%Y')

    #print(len(Raw),'reports found')

    Wells = (Raw.groupby(['API','Date'])
             .agg({'Gas':sum,'Liquid':sum,'Water':sum})
             .reset_index()
             #.groupby('API')
        )
    return Wells

def export_vol(Wells,outfile,header=None):
    if not header:
        header = """
*Field
*MONTHLY
*DAY *MONTH *YEAR *OIL *WATER *GAS 
"""

    # export data
    with open(outfile,'w') as f:
        f.write(header)
        for UWI,production in Wells.groupby('API'):
            f.write("\n*NAME {}\n".format(UWI))
            for _,vals in production.sort_values('Date').fillna(0).iterrows():
                f.write(f"1 {vals.Date.month:<2d} {vals.Date.year}   " + 
                        f"{vals.Liquid:<6.0f} {vals.Water:<6.0f} {vals.Gas:<6.0f}\n"
                       )
            # raise(StandardError("I don't care"))
    return

def export_injection_vol(Wells, outfile, header=None):
    if not header:
        header = """
*Field
*MONTHLY
*DAY *MONTH *YEAR *WATER *GAS 
"""
    with open(outfile,'w') as f:
        f.write(header)
        for UWI,production in Wells.groupby('API'):
            f.write("\n*NAME {}\n".format(UWI))
            for _,vals in production.sort_values('Date').fillna(0).iterrows():
                f.write(f"1 {vals.Date.month:<2d} {vals.Date.year}   " + 
                        f"{vals.Water:<6.0f} {vals.Gas:<6.0f}\n"
                       )
    return


if __name__=='__main__':
    # get arguments from command line
    parser = argparse.ArgumentParser(description="Convert production data from IHS format to Petrel format")
    parser.add_argument('infile',metavar="INFILE",nargs='+',help='IHS style input file')
    parser.add_argument('-o','--outfile',default='',help='Petrel style output file')
    parser.add_argument('-y','--yearly',action='store_true',help='Input and Output yearly production')
    parser.add_argument('-z','--zip',action='store_true',help='Unzip IHS file')
    args = parser.parse_args()

    infile = args.infile

    if not args.outfile:
        outfile=os.path.splitext(infile[0])[0]+'.vol'
    else:
        outfile = args.outfile

    if args.zip:
        from zipfile import ZipFile
        fin = args.infile[0]
        fzip = ZipFile(fin)
        inside_zip = (os.path.splitext(
            os.path.split(fin)[1])[0] +
                      "_Monthly Production.csv"
        )
        infile = fzip.open(inside_zip)

    wells = read_data(infile,yearly=args.yearly)
    export_vol(wells,outfile,header=None)
