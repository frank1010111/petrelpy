# Copyright 2011-2018 Frank Male
#This file is part of Fetkovich-Male fit which is released under a proprietary license
#See README.txt for details
import dask.dataframe as dd
import pandas as pd
import numpy as np
import fastparquet
from scipy.spatial import cKDTree

long_layerdict = {1:'above',2:'Upper Spraberry',3:'Middle Spraberry',4:'Lower Spraberry',5:'Middle Leonard',6:'Dean',
                 7:'Wolfcamp A',8:'Wolfcamp B',9:'Wolfcamp C1',10:'Wolfcamp C2',11:'Wolfcamp D',12:'Strawn'}

def load_from_petrel(fin,npartitions=60):
    numprops = pd.read_csv(fin,sep=' ',skiprows=1,nrows=1,header=None)[0][0]

    head = pd.read_csv(fin,sep=' ',skiprows=2,nrows=numprops,header=None)

    df = dd.read_csv(fin,
                     sep=' ',header=numprops+1,na_values=-999,
                    names=list(head[0])+['null']
    )
    df = df.repartition(npartitions=npartitions).drop('null',1)
    return df

def get_midpoint_cell_columns(df,dir_out):
    """Find cell columns where UWI-index exists
    df is the ini
    """
    #ID midpoints
    df_midpoints = df.dropna(subset=['UWI-index']).compute()
    
    #get cells where i or j match the midpoint i and j indexes
    df_i_or_j = (df[(df['i_index'].isin(df_midpoints['i_index'])) & 
              df['j_index'].isin(df_midpoints['j_index'])]
           )
    df_i_or_j.to_parquet(dir_out)
    
    # go through groups in the Parquet file as pandas dataframes,
    # grabbing cells that match i_index and j_index
    idx=pd.IndexSlice
    df_ij = []
    pf = fastparquet.ParquetFile(dir_out)
    n=1
    for dfp in pf.iter_row_groups():
        df_ij.append(
            dfp.dropna(thresh=3)
            .set_index(['i_index','j_index','k_index'])
            .sort_index()
            .loc[idx[df_midpoints['i_index'],df_midpoints['j_index'],:],])
        print(n,'row groups down')
        n+=1
    df_ij = pd.concat(df_ij)
    return df_ij  

def limit_column_height(df_midpoints,df_ij,zdmax=1000):
    """Cut down on size of model by limiting cell midpoint vertical space to within zdmax of distance
    between wells (in df_midpoints) and geomodel cells (in df_ij)
    """
    df_out = []

    for _,(i,j,z) in df_midpoints[['i_index','j_index','z_coord']].iterrows():
        ijmatch = df_ij.xs((i,j),level=(0,1),drop_level=False)
        #ijmatch = df_ij.xs(i,level=0,drop_level=False).xs(j,level=1,drop_level=False)

        df_out.append(
            ijmatch[abs(ijmatch['z_coord']-z)<zdmax]
        )
        #break
    df_out=pd.concat(df_out)
    return df_out


def get_header(fin):
    with open(fin,'r') as f:
        h = []
        i = False
        for line in f:
            line = line.strip()
            if line=='BEGIN HEADER':
                i = True
            elif line=='END HEADER':
                break
            elif i:
                h.append(line)
            else:
                #print(line)
                continue
    return h

def load_petrel_tops_file(fin):
    "Load in petrel tops file and return pandas dataframe"
    colnames = get_header(fin)
    with open(fin,'r') as f:
        for i,line in enumerate(f):
            line = line.strip()
            if line =='END HEADER':
                break
    df = pd.read_csv(fin,skiprows=i+1,names=colnames,sep='\s+',na_values=-999)
    return df
    
def match_well_to_cell(df_wells,df_cells,distance_upper_bound=2000):
    """Give each well in df_wells with an XYZ position the i,j,k index of the nearest cell in df_cells
    distance_upper_bound: limit of the max distance between the well and its cell
    """
    #make tree in 3D for getting nearest neighbors
    tree_cells = cKDTree(df_cells[['x_coord','y_coord','z_coord']])
    dist,locs = tree_cells.query(well_xyz.loc[:,['X','Y','Z']],k=1,
                                 distance_upper_bound=distance_upper_bound,n_jobs=4)
    print((~np.isfinite(dist)).sum(),'wells could not get matches')
    
    #make output dataframe
    well_cell = df_wells.loc[:,['Well','X','Y','Z']]
    for i in ('i_index','j_index','k_index'):
        well_cell.loc[:,i]=np.nan
        
    # assign i,j,k for each well to nearest cell
    for l,w in well_cell.iterrows():
        if np.isfinite(dist[l]):
            well_cell.loc[l,['i_index','j_index','k_index']]=df_cells.iloc[locs[l]].name  
    return well_cell

def match_ijz_petrel(df_full,UWI_toindex,wdir,zdmax=1000):
    """Puts together above functions to go from two dataframes to a merged dataframe
    inputs:
       df_full (dask.DataFrame): dask dataframe of the geomodel, must include a column called 'UWI-index'
                                 and ijk indices, and z coordinate
       UWI_toindex (pd.DataFrame): pandas dataframe with UWI-Index column and UWI column to do merging
       wdir (str): string pointing to the working directory for behind-the-scenes parquet file creation
       zdmax (float): maximum z variation from cells in ij column to point including a UWI-index
    outputs:
       merged (pd.DataFrame): merged dataframe that has all your favorite attributes in an ij column with 
                              less than zdmax vertical separation from midpoint at UWI-index

    """
    df_midpoints = df_full.dropna(subset=['UWI-index']).compute()
    df_ijmatched = gs.get_midpoint_cell_columns(df_full,wdir)
    df_ijmatched_zlim = gs.limit_column_height(df_midpoints,df_ijmatched,zdmax)
    UWI_to_index['UWI-Index']=UWI_to_index['UWI-Index'].astype(int)
    merged = (df_ijmatched_zlim.rename(columns={'UWI-index':'UWI-Index'})
          .reset_index()
          .merge(UWI_to_index,on='UWI-Index',how='left')
         )
    merged = merged[~merged.index.duplicated(keep='first')]
    return merged

def agg_ijz(df_ijmatched,df_midpoints=None,zdmax=10,agg_arg='mean',uwi_col='UWI-Index'):
    # df_wells = gs.agg_ijz(df_ijmatched_zlim,df_midpoints,agg_dict,'UWI-index')
    if df_midpoints is None:
        df_midpoints = df_ijmatched.dropna(subset=uwi_col)
    df_out_index = df_ijmatched[uwi_col].dropna().unique()
    df_out_columns = df_ijmatched.iloc[0:1].agg(agg_arg).columns
    df_out = pd.DataFrame(index=df_out_index,columns=df_out_columns)

    for _,(i,j,z,uwi) in df_midpoints[['i_index','j_index','z_coord',uwi_col]].iterrows():
        ijmatch = df_ijmatched.xs((i,j),level=(0,1),drop_level=False)
        zmatch = ijmatch[abs(ijmatch['z_coord']-z) < zdmax]
        agged = zmatch.agg(agg_arg).T.squeeze()
        df_out.loc[uwi] = zmatch.agg(agg_arg).iloc[0]
    return df_out



def get_facies_stats(df,zonename='Mainzones',faciesname='Facies',attrs=None):
    """Get aggregated statistics for different facies and zones"""
    
    df_out = df.groupby([zonename,faciesname])[list(attrs.keys())]
    df_out = df_out.agg(attrs).compute()
    return df_out
    
def get_facies_histograms(df,zone_name='Mainzones',facies_name='Facies',
                          properties=['Phi','Sw'],ooip_name='OOIP',
                          print_progress=True):
    zones = df[zone_name].unique().compute()
    facies = df[facies_name].unique().compute()

    ooip_splits = (
        pd.DataFrame(columns=pd.MultiIndex.from_product([zones.values,facies.values,properties],
                                                        names=['Zone','Facies','Property'])
        ).sort_index(level='columns')
    )

    # percentage of max for each property done over zone, facies
    prop_max = df[properties].max().compute()
    for z in zones:
        for face in facies:
            df_slice = df[(df[zone_name]==z) & (df[facies_name]==face)]
            for p in properties:
                df_slice[p+'_'] = ((df_slice[p]//(prop_max[p]/100.0))).dropna().astype(int)
                split = df_slice.groupby(p+'_')[ooip_name].sum().compute()
                ooip_splits.loc[:,(z,face,p)] = split
                if print_progress:
                    print('finished zone',z,'facies',face,'property',p)

    ooip_splits = ooip_splits.sort_index()
    #provide translation from index to x values for histogram
    index_conversion = pd.DataFrame(columns=properties,index=ooip_splits.index)
    for p in properties:
        index_conversion[p] = prop_max[p]*index_conversion.index/100.0

    return ooip_splits,index_conversion,prop_max

    
