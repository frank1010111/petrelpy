# Copyright 2011-2018 Frank Male
#This file is part of Fetkovich-Male fit which is released under a proprietary license
#See README.txt for details
import dask.dataframe as dd
import pandas as pd
import numpy as np
import fastparquet
import gslib

fin = 'OOIP facies dep.txt'
df = gslib.load_from_petrel(fin,npartitions=30)
df = (df.dropna(subset=['Mainzones'])
      .drop(['i_index','j_index','k_index','x_coord','y_coord','z_coord'],1)
      )

zone_name = 'Mainzones'
facies_name = 'Facies_BEG_DiscreteIK100K'
properties = ['PHITSGSfacies-dep','SWTSGSfacies-dep']
ooip_name = 'OOIP_Facies_dep'
aggregators = {'OOIP_Facies_dep':'sum','Bulkvolume':'sum','PHITSGSfacies-dep':'mean','SWTSGSfacies-dep':'mean'}

ooip = gslib.get_facies_stats(df,zone_name,facies_name,aggregators)
ooip.to_csv('OOIP_summary.csv')
print('saved OOIP summary')

ooip_splits,conversion,prop_max = gslib.get_facies_histograms(df,zone_name,facies_name,properties,ooip_name)

print('Calculated OOIP splits')
ooip_splits.to_csv('OOIP_hist.csv')
conversion.to_csv('OOIP_hist_converter.csv')
prop_max.to_csv('property_maxes.csv')