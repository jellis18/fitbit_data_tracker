from __future__ import division
import fitbit
import time
import sys
import datetime
import numpy as np
import pandas as pd
import private


class FitbitWrapper(object):
    
    def __init__(self, userid, client_secret, access_token, refresh_token):
        
        self.client = fitbit.Fitbit(userid, client_secret, oauth2=True, 
                                    access_token=access_token, 
                                    refresh_token=refresh_token)
        self.ntrials = 5
        
        # dictionaries for intraday timeseries
        self.idict = {'activities/heart': 'activities-heart-intraday', 
                      'activities/steps': 'activities-steps-intraday'}
        
    def make_date_list(self, start_date, end_date=None):
        
        # setup up time array
        start = pd.Timestamp(start_date)
        if end_date is None:
            end = pd.Timestamp(datetime.datetime.today().strftime('%Y-%m-%d'))
        else:
            end = pd.Timestamp(end_date)
        days = (end - start).days
        t = pd.to_datetime(np.linspace(start.value, end.value, days))
        date_list = map(lambda x: x.strftime('%Y-%m-%d'), t)
        
        return date_list
        
    def get_intraday_timeseries(self, start_date, end_date=None, 
                                series='activities/heart', level='1min'):
        
        date_list = self.make_date_list(start_date, end_date=end_date)
        
        # loop over dates and get series
        for ct, datestr in enumerate(date_list):
            
            # set up trial loop to catch rate limit
            for n in range(self.ntrials):
                try:
                    fitbit_stats = self.client.intraday_time_series(
                        series, base_date=datestr, detail_level=level)
                    break
                except Exception as e:
                    sys.stderr.write('Hit Rate limit, waiting 1 hour...\n')
                    time.sleep(3600)
                    continue
                    
            if ct == 0:
                df = pd.DataFrame.from_dict(fitbit_stats[self.idict[series]]['dataset'])
                df['time'] = pd.to_datetime(df['time'].apply(lambda x: datestr+' '+x))
            else:
                dftemp = pd.DataFrame.from_dict(fitbit_stats[self.idict[series]]['dataset'])
                dftemp['time'] = pd.to_datetime(dftemp['time'].apply(lambda x: datestr+' '+x))
                df = pd.concat([df, dftemp], ignore_index=True)
            
        return df
    
    def get_sleep(self, start_date, end_date=None):
    
        date_list = self.make_date_list(start_date, end_date=end_date)
    
        # loop over dates and get series
        date_of_sleep, efficiency, hours_asleep, asleep, awake = [], [], [], [], []
        for ct, datestr in enumerate(date_list):
            
            # set up trial loop to catch rate limit
            for n in range(self.ntrials):
                try:
                    sleep = self.client.sleep(date=datestr)
                    break
                except Exception as e:
                    sys.stderr.write('Hit Rate limit, waiting 1 hour...\n')
                    time.sleep(3600)
                    continue
            
            # check for sleep
            if len(sleep['sleep']) == 0:
                continue
                
            date_of_sleep.append(np.datetime64(sleep['sleep'][0]['dateOfSleep']).astype(datetime.datetime))
            efficiency.append(sleep['sleep'][0]['efficiency'])
            hours_asleep.append(sleep['sleep'][0]['minutesAsleep'] / 60)
            asleep.append(np.datetime64(sleep['sleep'][0]['startTime']).astype(datetime.datetime))
            awake.append(asleep[-1] + datetime.timedelta(minutes=sleep['sleep'][0]['minutesAsleep']+
                                                         sleep['sleep'][0]['minutesAwake']))
            
            # fix times
            asleep[-1] = asleep[-1].time()
            awake[-1] = awake[-1].time()
            
        df = pd.DataFrame({'date': date_of_sleep, 'efficiency': efficiency, 
                           'hours_asleep': hours_asleep, 'asleep_time': asleep, 
                           'awake_time': awake})
        
        return df
    
    def get_resting_hr(self, start_date, end_date=None):
        
        start = np.datetime64(start_date).astype(datetime.datetime)
        if end_date is None:
            end = datetime.datetime.today()
        else:
            end = np.datetime64(end_date).astype(datetime.datetime)
        
        hrt = self.client.time_series('activities/heart', base_date=start, 
                               end_date=end)

        rest = [h['value']['restingHeartRate'] for h in hrt['activities-heart']]
        dt = [h['dateTime'] for h in hrt['activities-heart']]

        df = pd.DataFrame({'date':dt, 'rest_hr':rest})
        df['date'] = pd.to_datetime(df['date'])
        
        return df

if __name__ == "__main__":

    start = '2016-03-08'
    fb = FitbitWrapper(private.USER_ID, private.CLIENT_SECRET, 
                       access_token=private.ACCESS_TOKEN, 
                       refresh_token=private.REFRESH_TOKEN)

    # resting heart rate
    rh_df = fb.get_resting_hr(start_date=start)
    rh_df.to_pickle('resting_hr.pkl')

    # sleep
    sleep_df = fb.get_sleep(start_date=start)
    sleep_df.to_pickle('sleep.pkl')

    # heart rate and steps intraday time series
    heart_df = fb.get_intraday_timeseries(start, series='activities/heart')
    heart_df.to_pickle('heart_timeseries.pkl')
    steps_df = fb.get_intraday_timeseries(start, series='activities/steps')
    steps_df.to_pickle('steps_timeseries.pkl')
