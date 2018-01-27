from dsutils.data_library import DataObject
import pandas as pd
import os
import sys
from dsutils import s3
import dsutils as ds
import numpy as np
import scipy as sp
from tqdm import tqdm as timeloop



#get similarity for single time point across different dimension instances
def get_distance(ref_locale,data):
    '''
    compute z-scored euclidean distances between ref_locale and all locales in data for a single time point
    each dimension instance comprises a dimension in euclidean space - so for a dataset with 3 different
    dimension instances, a distance would be computed in 3d space
    
    args:
        ref_locale (str): the reference locale (to be compared to)
        data (pandas dataframe): SLID-V dataframe containing the correct locale_class and time point
    
    returns:
        dists (pandas dataframe): contains two columns - locale & euclidean
    
    '''
    #make a copy of data
    df = data.copy()
    df['zscore'] = 0
    
    # calculate z-score for each dimension:
    dims = df.dimension_labels.unique()
    for dim in dims:
        #pull out that dimension instance
        temp = df[df.dimension_labels==dim]
        #calculate z-score
        zscores = (temp.value - temp.value.mean()) / temp.value.std()
        #add the z-score as a column into the copied dataframe
        df.loc[df.dimension_labels==dim,'zscore'] = zscores
    
    
    #calculate distance for each locale from WA
    euc = np.zeros(len(locales)) #initialize distance array
    
    u = df[df.locale==ref_locale].zscore.values #reference vector
    #test to see if data actually contains reference locale
    if u.shape[0] == 0:
        raise ValueError('No data for refence location ({})'.format(ref_locale))

    #loop through each locale, pull out the vector, compute euclidean distance
    for i,locale in enumerate(locales):
        v = df[df.locale == locale].zscore.values
        #test to see if data actually contains current locale
        if v.shape[0] == 0:
            raise ValueError('No data for {}'.format(locale))
        
        #compute euclidean distance
        euc[i] = sp.spatial.distance.euclidean(u,v)
    
    #add distance to the output dataframe
    dists = pd.DataFrame(data = {'locale':locales,'euclidean':euc})
    
    return dists.sort_values(by='euclidean')

def get_dists_times(df,ref_locale):
    '''
    computes distances over all the time points in the given dataset
    simply loops through each time and calls 'get_distance'
    
    args:
        df (pandas dataframe): dataframe with all timepoints
        ref_locale (str) : locale being compared against
    
    returns:
        df_dists (pandas dataframe): with 3 columns - 
            - locale
            - euclidean (distance)
            - interval
    '''
    #times to be looped through:
    times = df.interval.unique()
    
    #initialize distance dataframe
    df_dists = pd.DataFrame()

    for i,time in enumerate(times):
        #select the data for this time point
        d = df[df.interval == time]

        #compute distances
        df_temp = get_distance(ref_locale,d)
        df_temp['interval'] = time
        df_dists = df_dists.append(df_temp)
    
    return df_dists

def compute_corrs_dists(df,df_dists,ref_locale,min_times=3):
    '''
    computs correlation across time for each dimension instance, and translates it into a scalar to be applied 
    to the distance metrics
    
    args:
        - df (pandas dataframe): SLID-V data for all times and locales being considered
        - df_dists (pandas dataframe): contains distances for all time and locales (output of get_dists_times)
        - min_times (int): minimum number of timepoints neccesary to compute and include correlation
    
    returns:
        - df_scores (pandas dataframe): with the following columns:
            - locales
            - weighted_distance
            - raw_distance 
            - correlation
            - adj_correlation
    '''
    
    #get lists of locales and dimensions
    locales = df.locale.unique()
    dims = df.dimension_labels.unique()
    times = df.interval.unique()
    
    #initalize vectors for raw and overall distance scores
    dists = np.zeros(len(locales))
    scores = np.zeros(len(locales))
    
    if len(times) >= min_times:
    
        #initialize vector for correlations
        corrs = np.zeros(len(locales))

        for k,locale in enumerate(locales):
            #select the current locale
            d_ = df[df.locale == locale]

            temp_corrs = np.zeros(len(dims)) #initialize array for correlations

            for i,dim in enumerate(dims):
                #select the current dimension
                d = d_[d_.dimension_labels == dim]
                d_ref = df[((df.dimension_labels==dim) & (df.locale==ref_locale))]

                #test to see if two timeseries are same size
                ts_ref = d_ref.value.values
                ts_cpr = d.value.values

                if ts_ref.shape[0] != ts_cpr.shape[0]:
                    print('WARNING: {} does not have same interval data as {} for dimension {}'.format(locale,ref_locale,dim))
                    print('Distance from this location will not be computed \n')
                    temp_corrs[i] = None

                else:
                    #compute correlation
                    temp_corrs[i] = np.corrcoef(d_ref.value.values,d.value.values)[1][0]

            #compute average correlation
            corrs[k] = temp_corrs.mean()


            '''
            combine distance score with correlation score for each location

            Note on adjusting the correlation score:
                - correlation is [-1,1], but this isn't the form we want it in. 
                - want to scale the distance by correlation such that the distance between highly correlated locales
                is DECREASED, and vice versa. IE we want high correlations --> 0, and anti-correlations --> >1
                - we don't want the correlation scale to completely dominate the overall distance metric. 
                - for now, I settled on transforming hte correltion to be [0.5,1.5], where a correlation of 1 --> 0.5,
                and -1 --> 1.5. IE the correlation can at most change the distance by 50%. 
                - to do this, simply invert the correlation, divide by 2, and add 1. 
            '''
            #get distance for this locale
            dist = df_dists[df_dists.locale == locale].euclidean.mean()
            dists[k] = dist #just storing it for now

            #adjusting the correlation
            corr_scaled = (temp_corrs.mean()*-1 / 2) + 1.0 

            #apply adjusted correlation to distance for overall distance score
            scores[k] = dist*corr_scaled
    
        #apply adjustment to all correlations (just to display in the dataframe, can take this out later)
        corrs_scaled = (corrs*-1 / 2) + 1.0
    
    else:
        #there is not enough time points to include correlations
        print('Correlations not included. There are only {} time points'.format(len(times)))
        corrs,corrs_scaled = np.empty(len(locales))*np.nan,np.empty(len(locales))*np.nan
        
        for k,locale in enumerate(locales):
            #just put the 'raw' mean distance into scores
            dists[k] = df_dists[df_dists.locale == locale].euclidean.mean()
            
        scores = dists
    
    #store in output dataframe
    df_scores = pd.DataFrame(data = {'locales':locales,'weighted_distance':scores,'raw_distance':dists,'correlation':corrs,'adj_correlation':corrs_scaled})
    
    return df_scores.sort_values(by='weighted_distance')

def check_drop_locales(df):
    '''
    check to see if each locale contains the same dimension instances and time points
    if a location does not satisfy these requirements, it is dropped.
    
    args:
        df (pandas dataframe): SLID-V dataframe
    
    returns:
        df_out (pandas dataframe): same as df except for dropped locations
    
    note: this is called after the dataframe has been sliced to include the dimension instances and time 
    points that the reference location contains
    '''
    
    #get list of locales, dimensions, and times
    locales = df.locale.unique()
    dims = df.dimension_labels.unique()
    times = df.interval.unique()
    
    #keep track of the locales that satisfy both requirements
    good_locales = []
    
    #loop through each locale
    for locale in locales:
        curr = df[df.locale == locale]
        curr_dims = curr.dimension_labels.unique()
        curr_times = curr.interval.unique()
        
        #check if the dimension and time sets for this locale matches that of the entire dataset
        if ((set(curr_dims) != set(dims)) | (set(curr_times) != set(times))):
            print('WARNING: {} contains missing data and will not be included'.format(locale))
            
        else:
            good_locales.append(locale)
    
    #create a new dataframe comprised of only the locales that have all the dimension instances and time points
    df_out = df[df.locale.isin(good_locales)].copy()
    
    return df_out

def compare_locations(df_indicator, ref_locale, locale_class, dim_instances = 'all', interval = 'all'):
    '''
    computes overall distances across both time and dimensions for reference location against all 
    other locales in that local class
    
    args:
        df_indicator (pandas dataframe): SLID-V dataframe directly from S3
        ref_locale (str): locale to be referenced against
        locale_class (str): locale class to be compared
        dim_instances (list or default):a list of dimenstion instances to include. default is 'all', which
                                        will include all instances.
        interval (list or default): a list of times to include. default is 'all' which will include all times. 
        
    returns:
        df_scores (pandas dataframe): (this is the output of compute_corrs_dist())
        it contains the following columns:
            - locales
            - weighted_distance
            - raw_distance 
            - correlation
            - adj_correlation
        
    '''
    #copy into local dataframe
    df = df_indicator.copy()
    
    #select locale class <-- this will be removed in the future and should be done before function is called to
    #optimize for space
    df = df[df.locale_class == locale_class]
    
    #determine dimension instances to compare
    if dim_instances == 'all':
        dims = df.dimension_labels.unique()
    else:
        dims = dim_instances
        
    #determine intervals to compare
    if interval == 'all':
        times = df.interval.unique()
    else:
        times = interval
    
    #select out the appropriate dimension instances and intervals
    df = df[df.dimension_labels.isin(dims)]
    df = df[df.interval.isin(times)]
    
    '''
    test for missing data

    compare ref_locale times and dims with overall time and dims
        if different, then only use times and dims that the ref_locale has and warn the user that this happened
    '''
    ref_times = df[df.locale == ref_locale].interval.unique()
    ref_dims = df[df.locale == ref_locale].dimension_labels.unique()
    
    if set(ref_times) != set(times):
        #replace times with ref_times
        print('WARNING: {} does not contain data for all intervals in {}'.format(ref_locale, times))
        print('Only {} intervals will be included \n'.format(ref_times))
        times = ref_times
        df = df[df.interval.isin(times)]
        
    if set(ref_dims) != set(dims):
        #replace dims with ref_dims
        print('WARNING: {} does not contain data for all dimensions in {}'.format(ref_locale, dims))
        print('Only {} intervals will be included \n'.format(ref_dims))
        dims = ref_dims    
        df = df[df.dimension_labels.isin(dims)]
    
    '''
    at this point, we could check to see if:
        1. does each location have all the dimension instances for each time point?
        2. does each location have all the time points for each dimension instance?
    '''
    df = check_drop_locales(df)
    
    '''
    compute similarity score for each time point
    '''
    df_dists = get_dists_times(df,ref_locale)
    
    '''
    correlation for each dimension instance:
    '''
    df_scores = compute_corrs_dists(df,df_dists,ref_locale)
    
    return df_scores