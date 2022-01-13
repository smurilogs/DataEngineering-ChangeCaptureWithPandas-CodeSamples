
import pandas as pd
from datetime import datetime

class ChangeCapture():

    # stamps registers with certain columns to assign capture type, source and date tune
    @staticmethod
    def _stamp_captures(registers_df, capture_type, capture_source):
        
        # creates new columns in the df given the args
        registers_df['capture_type'] = capture_type
        registers_df['capture_source'] = capture_source    
        registers_df['capture_datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return registers_df

    # gets registers identified as inserted since the last state in a horizontal table
    @staticmethod
    def get_wide_inserts_df(curr_df, last_df, keys, source):
        
        # gets table with only the key columns of all inserts identified
        inserts_df = pd.merge(curr_df, last_df, on=keys, how='outer', indicator=True)
        inserts_df = inserts_df[inserts_df['_merge'] == 'left_only'].reset_index(drop=True)
        inserts_df = inserts_df[keys]

        # incorporates all non-key columns to the inserts identified
        inserts_df = pd.merge(inserts_df, curr_df, on=keys, how='left', indicator=True)
        inserts_df = inserts_df[inserts_df['_merge'] == 'both'].reset_index(drop=True)
        inserts_df = inserts_df.drop(columns=['_merge'])

        # stamps registers before returning them
        inserts_df = ChangeCapture._stamp_captures(inserts_df, 'INSERT', source)  
        return inserts_df

    # gets registers identified as inserted since the last state in a vertical table
    @staticmethod
    def get_long_inserts_df(curr_df, last_df, keys, source):
        
        # captures horizontal table and makes it vertical
        wide_inserts_df = ChangeCapture.get_wide_inserts_df(curr_df, last_df, keys, source)
        non_keys = list(set(curr_df.columns.tolist()) - set(keys))
        inserts_df = wide_inserts_df.melt(id_vars=keys, value_vars=non_keys, var_name='column_name', value_name='value_stored')

        # stamps registers before returning them
        inserts_df = ChangeCapture._stamp_captures(inserts_df, 'INSERT', source)  
        return inserts_df

    # gets registers identified as deleted since the last state in a horizontal table
    @staticmethod
    def get_wide_deletes_df(curr_df, last_df, keys, source):
        
        # gets table with only the key columns of all deletes identified
        deletes_df = pd.merge(curr_df, last_df, on=keys, how='outer', indicator=True)
        deletes_df = deletes_df[deletes_df['_merge'] == 'right_only'].reset_index(drop=True)
        deletes_df = deletes_df[keys]

        # incorporates all non-key columns to the deletes identified
        deletes_df = pd.merge(deletes_df, last_df, on=keys, how='left', indicator=True)
        deletes_df = deletes_df[deletes_df['_merge'] == 'both'].reset_index(drop=True)
        deletes_df = deletes_df.drop(columns=['_merge'])

        # stamps registers before returning them
        deletes_df = ChangeCapture._stamp_captures(deletes_df, 'DELETE', source)  
        return deletes_df

    # gets registers identified as deleted since the last state in a vertical table
    @staticmethod
    def get_long_deletes_df(curr_df, last_df, keys, source):
        
        # captures horizontal table and makes it vertical
        wide_deletes_df = ChangeCapture.get_wide_deletes_df(curr_df, last_df, keys, source)
        non_keys = list(set(curr_df.columns.tolist()) - set(keys))
        deletes_df = wide_deletes_df.melt(id_vars=keys, value_vars=non_keys, var_name='column_name', value_name='value_stored')

        # stamps registers before returning them
        deletes_df = ChangeCapture._stamp_captures(deletes_df, 'DELETE', source)    
        return deletes_df

    # gets registers identified as updated since the last state in a horizontal table
    @staticmethod
    def get_wide_updates_df(curr_df, last_df, keys, source):

        # gets rows with both keys and non-keys values in common
        same_row_df = pd.merge(curr_df, last_df, on=last_df.columns.tolist(), how='outer', indicator=True)
        same_row_df = same_row_df[same_row_df['_merge'] == 'both'].reset_index(drop=True)
        same_row_df = same_row_df[keys]

        # gets rows with only keys values in common
        same_keys_df = pd.merge(curr_df, last_df, on=keys, how='outer', indicator=True)
        same_keys_df = same_keys_df[same_keys_df['_merge'] == 'both'].reset_index(drop=True)
        same_keys_df = same_keys_df[keys]

        # selects the rows that have differences comparing current and last states
        updates_df = pd.merge(same_row_df, same_keys_df, on=keys, how='outer', indicator=True)
        updates_df = updates_df[updates_df['_merge'] == 'right_only'].reset_index(drop=True)
        updates_df = updates_df.drop(columns=['_merge'])

        # incorporates all non-key columns to the updates identified
        updates_df = pd.merge(updates_df, curr_df, on=keys, how='left', indicator=True)
        updates_df = updates_df[updates_df['_merge'] == 'both'].reset_index(drop=True)
        updates_df = updates_df.drop(columns=['_merge'])

        # stamps registers before returning them
        updates_df = ChangeCapture._stamp_captures(updates_df, 'UPDATE', source)  
        return updates_df

    # gets registers identified as updated since the last state in a vertical table
    @staticmethod
    def get_long_updates_df(curr_df, last_df, keys, source):
        
        wide_updates_df = ChangeCapture.get_wide_updates_df(curr_df, last_df, keys, source)
        non_keys = list(set(curr_df.columns.tolist()) - set(keys))
        new_updates_df = wide_updates_df.melt(id_vars=keys, value_vars=non_keys, var_name='column_name', value_name='value_stored')
        old_updates_df = last_df.melt(id_vars=keys, value_vars=non_keys, var_name='column_name', value_name='value_stored')
        
        updates_df = pd.merge(new_updates_df, old_updates_df, on=new_updates_df.columns.tolist(), how='outer', indicator=True)
        updates_df = updates_df[updates_df['_merge'] == 'left_only'].reset_index(drop=True)
        updates_df = updates_df.drop(columns=['_merge'])

        # stamps registers before returning them
        updates_df = ChangeCapture._stamp_captures(updates_df, 'UPDATE', source)    
        return updates_df

    # checks if there's any change since last state
    @staticmethod
    def has_changed(curr_df, last_df):
        
        changes_df = pd.merge(curr_df, last_df, on=curr_df.columns.tolist(), how='outer', indicator=True)
        changes_df = changes_df[(changes_df['_merge'] == 'left_only')|(changes_df['_merge'] == 'right_only')].reset_index(drop=True)
        if(len(changes_df) > 0):
            return True
        return False
