#convert_production_petrel.py
# A script for converting IHS completion data into a format readable by
# Petrel in the .ev format
# Copyright 2018 Frank Male


#A .ev file is supposed to look like this:
"""
UNITS FIELD

WELLNAME D1
12.07.1995 perforation 4075 4190 0.700833 012.07.1995 perforation 4200
4290 0.700833 012.07.1995 perforation 4300 4450 0.700833 001.06.2003
squeeze 4075 4190

WELLNAME D217.07.1995 perforation 5340 5570 0.700833 0
17.07.1995 perforation 5580 5695 0.700833 0

WELLNAME D6Z
02.07.1995 perforation 6475 6680 0.575 0
02.07.1995 perforation 6690 6846 0.575 0
02.07.1995 perforation 6856 7386 0.575 0
01.12.1996 plug 6856
01.12.1996 plug 6690 
01.12.1996 plug 6475
"""

import pandas as pd
import numpy as np
import argparse
import os

def collect_perfs(df_perf):
    #gather dates
    df_perf['Date'] = df_perf['Date Completion']
    df_perf.loc[df_perf.Date.isna(),'Date'] = df_perf['Date First Report']
    df_perf['Date'] = pd.to_datetime(df_perf['Date'])
    
    out_df = (df_perf[['Depth Top','Depth Base','Date']]
              .dropna()
               .assign(Date = lambda x: x.Date.dt.strftime('%m.%d.%Y'))
               .groupby(level=[0])
             )
    return out_df

def export_perfs(out_df,out_fname,header=None):
    if not header:
        header =  """UNITS FIELD\n"""
    #print(header)
    with open(out_fname,'w') as f:
        f.write(header)
        for api,well in out_df:
            f.write(f"\nWELLNAME {api}\n")
            for _,vals in well.iterrows():
                f.write(f"{vals['Date']} perforation {vals['Depth Top']} {vals['Depth Base']} 1 0\n")
                        

                        
if __name__ =='__main__':
    # get arguments from command line
    parser = argparse.ArgumentParser(description="Convert perforation data from IHS format to Petrel-readable .ev format")
    parser.add_argument('infile', metavar="INFILE", help='IHS style input file',nargs='+')
    parser.add_argument('-o','--outfile', default='', help='Petrel .ev style output file')
    parser.add_argument('--header', default='', help='file to read in for header information')
    parser.add_argument('--sheetname',default=0,help='sheet name for excel file inputs')
    parser.add_argument('--date_col',default='Treatment Start Date',help='column for perforation dates')
    args = parser.parse_args()


    infile = args.infile
    sheetname = args.sheetname
    if not args.outfile:
        outfile=os.path.splitext(infile[0])[0]+'.ev'
    else:
        outfile = args.outfile
    if args.header:
        header = args.header
    else:
        header = """\
    UNITS FIELD
    """


    # read in data and fix column names
    if '.xls' in infile[0]:
        Raw = pd.concat([pd.read_excel(i,sheetname,index_col=0) for i in infile])
    elif '.prn' in i:
        Raw = pd.concat([pd.read_csv(i,sep='\s+',index_col=0) for i in infile])
    else:
        Raw = pd.concat([pd.read_csv(i,index_col=0) for i in infile])

    print(len(Raw),'reports found')


    Raw.rename(columns={'Pressure Instantaneous Shutin (PSIG)':"pressure_shutin",
                        'Depth Top':'depth_top','Depth Base':'depth_base'},
               inplace=True)
    assert args.date_col in Raw.columns, f"Date column not found: {args.date_col}"

    Raw['Date'] = Raw[args.date_col].dt.strftime('%m.%d.%Y')

    Wells = Raw.groupby(level=0)

    # export data
    with open(outfile,'wb') as f:
        f.write(header)
        for API,well in Wells:
            f.write("\nWELLNAME {}\n".format(API))
            for _,vals in well.iterrows():
                f.write("{} perforation {} {} 1 0\n".format(*vals[['Date', 'start_depth', 'stop_depth']]))


# # export PRN
# PRN = (Raw.rename(columns={'API':'UWI','start_depth':'Top','stop_depth':'Bottom'})
#        [['UWI','Top','Bottom']]
#        .assign(Perf=1)
#        .sort_values('UWI')
# )
# n=0
# with open(outfile,'wb') as f:
#     f.write("UWI             Top    Bottom  Perf\n")
#     for uwi,w in PRN.groupby('UWI'):
#         oldb = 0
#         for i,x in w.iterrows():
#             t,b = x[['Top','Bottom']]
#             if t<= oldb:
#                 n+=1
#                 t=oldb+1
#                 if b<t:
#                     b=t
#             oldb = b
#             f.write("{}  {:<6.0f} {:<6.0f}  1\n".format(uwi,t,b))

# print n,'corrections'
                        
# Wells = Raw.groupby('API',sort=False)

# # export data
# with open(outfile,'wb') as f:
#     f.write(header)
#     for API,well in Wells:
#         f.write("\nWELLNAME {}\n".format(API))
#         for _,vals in well.iterrows():
#             f.write("{} {} {} {} {} 0\n".format(*vals[['date','event','start_depth','stop_depth','efficiency']]))

# Raw.columns = ('API','start_depth','stop_depth','date','status')