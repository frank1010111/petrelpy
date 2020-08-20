import pandas as pd
from petrelpy.tops_attributes import read_tops

def write_header(df, fname, fill_na=-999):
    header_head = '''# Petrel well head
VERSION 1
BEGIN HEADER
''' + '\n'.join(df.columns) + '\nEND HEADER\n'
    body = (df.fillna(fill_na)
            .to_csv(header=False, index=False, quoting=2, sep=' ', line_terminator='\n')
            )
    with open(fname, 'w') as f:
        f.write(header_head+ body)

#def write_header_csv(df, fname, fillna=-999):
#    (df.fillna(-999)
#     .to_csv(fname

def read_header(fname):
    return read_tops(fname)
        
