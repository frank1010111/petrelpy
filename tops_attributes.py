#tops_attributes.py
#Utility for reading Petrel tops files and putting dataframes into Petrel-readable tops files
#Useful for reverse-engineering certain well attributes
import pandas as pd

def read_tops(fname):
    with open(fname,'r') as f:
        i = 0
        cols_started = False
        colnames = []
        for line in f:
            line = line.rstrip('\r\n')
            if line == 'END HEADER':
                break
            else:
                i += 1
                if cols_started:
                    colnames.append(line)
                if line == 'BEGIN HEADER':
                    cols_started = True
    try:
        df = pd.read_csv(fname, skiprows=i+1, names=colnames, header=None, sep='\s+',
                         na_values=-999, dtype={'Well':str})
    except UnicodeDecodeError:
        df = pd.read_csv(fname, skiprows=i+1, names=colnames, header=None, sep='\s+',
                         na_values=-999, dtype={'Well':str}, encoding = "ISO-8859-1")
    return df

def write_tops(df, fname, comments="", fill_na=-999):
    header = ('BEGIN HEADER\n' +
              '\n'.join(df.columns) +
              '\nEND HEADER\n')
    body = (df.fillna(fill_na)
            .to_csv(header=False, index=False, quoting=2, sep=' ', line_terminator='\n')
           )
    with open(fname,'w') as f:
        f.write(comments + '\nVERSION 2\n' +
               header +
               body
               )
