import pandas as pd
import io

COL_NAMES_TRAJECTORY = [
    'MD_ENTRY', 
    'GRID_I', 
    'GRID_J', 
    'GRID_K',
    'WELL_ENTRY_X',
    'WELL_ENTRY_Y',
    'WELL_ENTRY_Z',
    'ENTRY_FACE',
    'MD_EXIT',
    'WELL_EXIT_X',
    'WELL_EXIT_Y',
    'WELL_EXIT_Z',
    'EXIT_FACE',
]

def process_well_connection_file(well_connection_file: str, wellname_to_heel: pd.DataFrame, property_aggregates: dict, col_names: list) -> pd.DataFrame:
    """Get average properties along the laterals for a well connection file
    
    Paramters
    ---------
    well_connection_file: str
        Eclipse well connection file exported from Petrel
    wellname_to_heel: pd.DataFrame
        DataFrame containining UWI,Name,Depth_Heel for each well in the connection file
    property_aggregates: dict
        properties from the well connection file to extract and how to aggregate them
    col_names: list
        dictionary of column names in the well connection file
        you can check this with get_tragectory_columns(well_connection_file)
        the first few are usually ['MD_ENTRY', 'GRID_I', 'GRID_J', 'GRID_K','WELL_ENTRY_X','WELL_ENTRY_Y',
        'WELL_ENTRY_Z','ENTRY_FACE','MD_EXIT','WELL_EXIT_X','WELL_EXIT_Y','WELL_EXIT_Z','EXIT_FACE',]
    
    Output: pd.DataFrame
        DataFrame indexed by UWI, with columns that are the keys of property_aggregates
    Note: vertical wells get everything included, wells not in wellname_to_heel are treated as vertical and have their wellname as their index
    """
    with open(well_connection_file) as f:
        property_frame = pd.DataFrame(
            [process_well_lateral(ws, wellname_to_heel, property_aggregates, col_names) for ws in get_well(f)]
        )
    return property_frame

def get_wellnames(wc_file):
    "Get all the well names from a well connection file"
    with open(wc_file, 'r') as f:
        wellnames = [row.replace('WELLNAME',"").strip() for row in f if row.startswith('WELLNAME')]
    return wellnames

def get_trajectory_columns(fname):
    with open(fname,'r') as f:
        trajectory = False
        columns = ''
        for row in f:
            #print(row)
            if trajectory:
                columns = row.strip()
                break
            if row.strip() == 'TRAJECTORY_COLUMN_ORDER':
                trajectory = True
    return columns

def process_well_lateral(well_string:str, wellname_to_heel: pd.DataFrame, property_aggregates: dict, col_names: list) -> pd.Series:
    wellname = get_wellname(well_string)
    df = get_trajectory(well_string, col_names)
    try:
        uwi_heel = wellname_to_heel[wellname_to_heel['Name'] == wellname].iloc[0]
    except:
        uwi_heel = pd.Series({'UWI': wellname, 'Depth_heel':0})
    if df.MD_ENTRY.max() < uwi_heel['Depth_heel']:
        filtered_df = df.iloc[[-1]]
    else:
        filtered_df = df[df.MD_ENTRY >= uwi_heel['Depth_heel']]
    properties = filtered_df.agg(property_aggregates)
    if properties.shape[0] == 0:
        properties = properties.reindex([0])
    properties = properties.iloc[0].rename(uwi_heel['UWI'])
    return properties

def get_trajectory(well_string, col_names) -> pd.DataFrame:
    trajectory = well_string.split('TRAJECTORY')[1]
    if 'END_' in trajectory:
        trajectory = trajectory.split("END_")[0]
    out_frame = pd.read_csv(io.StringIO(trajectory), sep='\s+', names=col_names, na_values=['-999'])
    return out_frame

def get_well(file_obj):
    in_well = False
    strings = ''
    for row in file_obj:
        if row.startswith('WELLNAME'):
            in_well = True
            strings += row
        elif row.startswith('END_TRAJECTORY') and in_well:
            in_well = False
            strings += row
            out_string = strings
            strings = ''
            yield out_string
        elif in_well:
            strings += row

def get_wellname(well_string: str) -> str:
    wellname = well_string.split('\n', 2)[0].replace('WELLNAME','').strip()
    return wellname

