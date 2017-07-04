def main():
    # importing required modules
    import pandas as pd
    import numpy as np
    import requests
    import json
    import datetime

    date_today = datetime.date.today().isoformat()
    # getting user input for URLs
    while True:
        update_val = input("Updating TCdata360 (write 'T'), Govdata360 ('G'), or custom URLs ('C')?")
        if update_val == 'T':
            site_type = 'TCdata360'
            nav_url = "http://tcdata360-backend.worldbank.org/api/v1/nav/all"
            ind_url = "http://tcdata360-backend.worldbank.org/api/v1/indicators/"
            print("Generating the updated indicator hierarchy file for %s as of %s." % (site_type, date_today))
            break

        elif update_val == 'G':
            site_type = 'Govdata360'
            nav_url = "http://govdata360-backend.worldbank.org/api/v1/nav/all"
            ind_url = "http://govdata360-backend.worldbank.org/api/v1/indicators/"
            print("Generating the updated indicator hierarchy file for %s as of %s." % (site_type, date_today))
            break

        elif update_val == 'C':
            site_type = 'Custom'
            nav_url = input("Please write the full URL here for the nav/all dataset.")
            ind_url = input("Please write the full URL here for the indicators dataset.")
            print("""Generating the updated indicator hierarchy file for the inputted URLs as of %s.
            Note that this may fail if the passed URLs are invalid or not in the correct format.""" % date_today)
            break
        else:
            print("Invalid input. Please write T, G, or C only.")


    # Generate flat nav hierarchy dataset from `/nav/all`
    response = requests.get(nav_url)

    level = 0
    json_col = 'children'
    df = pd.io.json.json_normalize(response.json())
    df.columns = ["level%d." % level + col for col in df.columns]
    df['indicator.name'] = df["level%d.name" % level]
    df['indicator.id'] = df["level%d.id" % level]
    df['indicator.rank'] = df["level%d.rank" % level]
    df['indicator.slug'] = df["level%d.slug" % level]
    temp_df = df
    check_val = sum(df['level%d.children' % (level)].apply(lambda x: True if type(x) is list else False))

    while check_val > 0:
        # print("Generating nested hierarchy dataset for %s level %d with %d non-NULL records" % (json_col, level, check_val))

        temp_df2 = pd.DataFrame()
        for row in df.index:
            if type(df.loc[row, 'level%d.children' % (level)]) is list:
                temp_df_row = pd.io.json.json_normalize(df.loc[row, 'level%d.children' % (level)])
                temp_df_row.columns = ['level%d.' % (level + 1) + col for col in temp_df_row.columns]
                temp_df_row['level%d.id' % (level)] = df.loc[row, 'level%d.id' % (level)]
                temp_df2 = temp_df2.append(temp_df_row)

        temp_df2.reset_index(drop=True, inplace=True)
        temp_df2 = temp_df2.merge(
            df.drop(['indicator.name', 'indicator.id', 'indicator.rank', 'indicator.slug'], axis=1), how='left',
            on='level%d.id' % (level))
        temp_df2['indicator.name'] = temp_df2["level%d.name" % (level + 1)]
        temp_df2['indicator.id'] = temp_df2["level%d.id" % (level + 1)]
        temp_df2['indicator.rank'] = temp_df2["level%d.rank" % (level + 1)]
        temp_df2['indicator.slug'] = temp_df2["level%d.slug" % (level + 1)]

        df = df.append(temp_df2)
        df.reset_index(drop=True, inplace=True)

        level += 1
        # print("Resulting dataframe has %d records and %d columns." % (df.shape[0], df.shape[1]))
        try:
            check_val = sum(
                df['level%d.children' % (level)].apply(lambda x: True if type(x) is list else False))
        except:
            children_cols = [col for col in df.columns if 'children' in col]
            df_hierarchy = df.drop(children_cols, axis=1)
            break

    # Generate indicator dataset from `/indicators/`
    response = requests.get(ind_url)
    df_indicators = pd.read_json(response.text)

    # Generate indicator overlap statistics
    ind_set = set(df_indicators['name'])
    nav_set = set(df_hierarchy['indicator.name'])

    set_int = len(set(df_hierarchy['indicator.name']) & set(df_indicators['name']))
    nav_diff = len(set(df_hierarchy['indicator.name']) - set(df_indicators['name']))
    ind_diff = len(set(df_indicators['name']) - set(df_hierarchy['indicator.name']))
    print()
    print("""There are %d overlaps between nav and indicators.
    %d are in nav but not in indicators.
    %d are in indicators but not in nav.""" % (set_int, nav_diff, ind_diff))

    # Generate separate and merged nav hierarchy and indicator datasets
    df_indicators.columns = ['indicator_' + col for col in df_indicators.columns]
    df_hierarchy.columns = ['nav_' + col for col in df_hierarchy.columns]

    ind_filename = "%s-%s_indicator_dataset.csv" % (date_today, site_type)
    df_indicators.reset_index(drop=True).to_csv(ind_filename)
    print("Finished generating %s with %d rows and %d columns." %
          (ind_filename, df_indicators.shape[0],df_indicators.shape[1]))

    nav_filename = "%s-%s_nav_hierarchy_dataset.csv" % (date_today, site_type)
    df_hierarchy.reset_index(drop=True).to_csv(nav_filename)
    print("Finished generating %s with %d rows and %d columns." %
          (nav_filename, df_hierarchy.shape[0], df_hierarchy.shape[1]))

    df_union = df_indicators.merge(df_hierarchy, how='outer', left_on='indicator_name', right_on='nav_indicator.name')
    union_filename = "%s-%s_merged_indicators_and_nav_hierarchy_dataset.csv" % (date_today, site_type)
    df_union.reset_index(drop=True).to_csv(union_filename)
    print("Finished generating %s with %d rows and %d columns." %
          (union_filename, df_union.shape[0], df_union.shape[1]))

if __name__ == '__main__':
    main()
