# Copyright 2011-2018 Frank Male
#This file is part of Fetkovich-Male fit which is released under a proprietary license
#See README.txt for details

import pandas as pd

def propdist_robintoguin(fin,fout,prop):
    df = pd.read_csv(fin,sep=' ',header=8,index_col=False,
                 names=['i','j','k','x_coord','y_coord','z_coord',prop])
    layernum =df.k.unique()
    if len(layernum)==10:

        df['layer'] = df.k.replace({1:"USB",2:"MSB",3:"LSB",4:"ML",5:"Dean",6:"WCA",
                                    7:"WCB",8:"WCC1",9:"WCC2",10:"WCD"})
    elif len(layernum)==12:
        df['layer'] = df.k.replace({1:'above',2:"USB",3:"MSB",4:"LSB",5:"ML",
                                    6:"Dean",7:"WCA",8:"WCB",9:"WCC1",10:"WCC2",
                                    11:"WCD",12:'STR'})
    else:
        raise(ValueError('the number of k-layers is not 10 or 12, it\'s {}'.format(layernum)))

    dfo =df.set_index(['x_coord','y_coord','layer'])[[prop]].unstack('layer')
    dfo.columns=['_'.join(c) for c in dfo.columns.values]
    dfo.to_csv(fout)


def main():
    #Pressure
    propdist_robintoguin('/home/malef/Downloads/All TORA Pressure XYZ Gslib Midland Basin.txt',
                         '/home/malef/West Texas data/Pressure.csv','Press')
    #API gravity
    propdist_robintoguin('/home/malef/Downloads/All TORA API gravity from Midpoints XYZ Gslib Midland Basin.txt',
                         '/home/malef/West Texas data/Gravity.csv','API')
    #water saturation
    propdist_robintoguin('/home/malef/Downloads/All TORA Water Saturation SWT XYZ Gslib Midland Basin.txt',
                        '/home/malef/West Texas data/Sw.csv',
                        'SW')

    # z-values
    df = pd.read_csv('/home/malef/Downloads/All TORA API gravity from Midpoints XYZ Gslib Midland Basin.txt',
                    sep=' ',header=8,index_col=False,
                     names=['i','j','k','x_coord','y_coord','Z','API'])
    df['layer'] = df.k.replace({1:"USB",2:"MSB",3:"LSB",4:"ML",5:"Dean",6:"WCA",
                                7:"WCB",8:"WCC1",9:"WCC2",10:"WCD"})
    dfo =df.set_index(['x_coord','y_coord','layer'])[['Z']].unstack('layer')
    dfo.columns=['_'.join(c) for c in dfo.columns.values]
    dfo.to_csv('/home/malef/West Texas data/Z.csv')

if __name__=='__main__':
    # main()
    # #Isochore
    # propdist_robintoguin('../WestTexas_raw/All TORA Isochore XYZ Gslib Midland Basin.txt',
    #                      '../WestTexas_raw/Isochore.csv',
    #                      'D')
    
    # #PhiT
    # propdist_robintoguin('../WestTexas_raw/All TORA Porosity PHIT XYZ Gslib Midland Basin.txt',
    #                      '../WestTexas_raw/PhiT.csv',
    #                      'PhiT')

    # #Ro
    propdist_robintoguin('../WestTexas_raw/All TORA Vitrinite Reflection Ro XYZ Gslib Midland Basin.txt',
                         '../WestTexas_raw/Vitrinite_reflectance.csv',
                         'Ro')
