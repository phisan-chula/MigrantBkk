#
#
#
#
#
import pandas as pd 
import geopandas as gpd
from pathlib import Path
import random
import time,sys
from functools import partial

############################################################################
def _RW_CACHE_( CACHE , FUNC=None): 
    ''' a caching system for serializing data structure or object into
        non-volatile storage and for later retrieval efficiently'''
    FRAME = sys._getframe().f_code.co_name
    ''' read or write cache file according to its existence'''
    if Path( CACHE ).is_file():
        t = time.process_time()
        print(f'{FRAME}(): Reading cache "{CACHE}"...')
        DF = pd.read_pickle( CACHE )
        elaps_t = (time.process_time() - t)
        print(f'{FRAME}(): ..."{CACHE}" elapsed time : {elaps_t:.1f} sec.')
    else:
        if not CACHE.parents[0].is_dir():
            CACHE.parents[0].mkdir( parents=True, exist_ok=Ture )
        print(f'{FRAME}(): Writing new Cache "{CACHE}"...')
        DF = FUNC()
        DF.to_pickle(CACHE, compression='infer' )  
    return DF 

############################################################################
def Read_FBAI_BBOX( TOTAL_BOUND ):
    FBAI = [ r'~/GeoData/FB_AI_Demog/2020/tha_men_2020.csv', 
             r'~/GeoData/FB_AI_Demog/2020/tha_women_2020.csv' ]
    minx, miny, maxx, maxy = TOTAL_BOUND
    dfs = list()
    #import pdb; pdb.set_trace()
    for f in FBAI:
        df = pd.read_csv( f )
        df = df[ (df.longitude>minx) & (df.longitude<maxx) &
                 (df.latitude >miny) & (df.latitude <maxy)  ].copy()
        dfs.append( df )
    df_popu = pd.concat( dfs, axis=1, join='inner' )
    df_popu = df_popu.loc[:, ~df_popu.columns.duplicated()]  # remove duplicated columns
    df_popu['tha_pop_2020' ] = df_popu['tha_men_2020']+df_popu['tha_women_2020']
    df_popu = gpd.GeoDataFrame( df_popu, crs='EPSG:4326',
           geometry=gpd.points_from_xy( df_popu.longitude, df_popu.latitude ) )
    return df_popu

############################################################################
def ComparePopu( dfDT, dfPopu,  DORUN=False ): 
    assert( dfDT.crs == dfPopu.crs )
    cmp = list()
    for i in range( len(dfDT) ):
        t = time.process_time()
        dt = dfDT.iloc[i:i+1]
        xmin, ymin, xmax, ymax = dt.total_bounds
        dts = dt.iloc[0]
        if DORUN:  # actual run !
            df = dfPopu.cx[xmin:xmax, ymin:ymax].copy()   # BBox for district
            PopDT = gpd.sjoin( df, dt[['dcode','dname_e','geometry']] , 
                    how='inner', predicate='intersects' )  
            distr =  dts.dname_e
            popmig = int( PopDT.tha_pop_2020.sum())
            popreg = dts.no_male+dts.no_female 
        else:  # for debug
            distr=f'test_{i}'; popreg=77_720 ; popmig=random.randint(88_000,170_000)
        diff = popmig-popreg
        diff_perc = '{:.1f}'.format( 100*diff/popreg )
        elapsed_time = (time.process_time() - t)
        print( f'| {i:02d} | {dts.dcode:6s} | {distr:20} | {popmig:8,} |'\
               f' {popreg:8,} | {diff:8,} ({diff_perc:5}%) |'\
               f' {dts.area_sqm/1E6:5.1f} | {elapsed_time:.0f}s. |')
        cmp.append( [ i, dts.dcode, distr, popmig, 
                      popreg, diff, diff_perc,
                      dts.area_sqm, elapsed_time] )
    return pd.DataFrame( cmp, columns= [ 'index', 'dcode',  'distr', 
        'popmig', 'popreg', 'diff' , 'diff_perc',  'area_sqm', 'elapsed_time'] )

############################################################################
def PlotMigr( dfDT, dfPopu, DIFF='diff' ):
    import matplotlib.pyplot as plt
    from matplotlib import cm
    df = pd.concat( [dfDT[['dcode', 'geometry']], dfPopu[[DIFF,]]], axis=1 )
    df_label = df.copy()
    df_label['geometry'] = df_label['geometry'].representative_point()
    fig, ax = plt.subplots(1,1, figsize=(20, 12))
    sm = plt.cm.ScalarMappable(cmap="jet", norm=plt.Normalize(
                     vmin=df[DIFF].min(), vmax=df[DIFF].max()))
    sm.set_array([])
    df.plot(DIFF,  cmap='jet', ax=ax, alpha=0.7)
    ax.set_axis_off()
    plt.colorbar(sm, alpha=0.7)
    for i,row in df_label.iterrows():
        ax.text( row.geometry.x, row.geometry.y, row.dcode,
                horizontalalignment='center', verticalalignment='center', size=18 )
    ax.set_title(f'"{DIFF}" Migrant Population of Bangkok (2020)')
    fig.tight_layout()
    plt.savefig(f'CACHE/MigrantBKK_{DIFF}.png')
    #plt.show()
    return

#########################################################################
#########################################################################
#########################################################################
DSTR = r'../BMA20k/District/district.shp'
dfDT = gpd.read_file( DSTR, encoding='TIS-620' )  # UTM z47
def Calc(row):
    return row.geometry.area
dfDT['area_sqm'] = dfDT.apply( Calc, axis='columns' )

#########################################################################
CacheBB_BKK = Path( 'CACHE/df_popu_BB_BKK.bz2' )
dfDTgeo = dfDT.to_crs('epsg:4326')
dfPopu = _RW_CACHE_( CacheBB_BKK, 
            partial( Read_FBAI_BBOX, dfDTgeo.total_bounds ) )   # BBox for BMA
dfPopu = dfPopu.to_crs('epsg:32647')
#########################################################################
CacheCMP = Path( 'CACHE/df_Cache_Cmp.bz2' )
dfCmp =  _RW_CACHE_( CacheCMP,
                partial( ComparePopu, dfDT, dfPopu, DORUN=True ) )
#########################################################################
PlotMigr( dfDT, dfCmp, DIFF='diff' )
PlotMigr( dfDT, dfCmp, DIFF='diff_perc' )
#import pdb; pdb.set_trace()
