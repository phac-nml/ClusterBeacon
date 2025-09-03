from sklearn.ensemble import IsolationForest
import pandas as pd

def isolation_forest(df):
    df_without_index = df.reset_index(drop=True)
    model = IsolationForest(bootstrap=True,contamination=0.1, max_samples=0.2)
    model.fit(df_without_index)
    anomalies = pd.Series(model.predict(df_without_index)).apply(lambda x: True if (x == -1) else False)
    return anomalies


def load_data(filename,label_col,date_col):
    df = pd.read_csv(filename,sep="\t",header=0)
    df = df.dropna(subset = [date_col,label_col])
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.dropna(subset = [date_col])
    df['year-week'] = df['date'].dt.strftime('%Y-%U')
    return df.groupby(['year-week',label_col]).size().reset_index(name='count')

def process(df,label_col,min_date,max_date,date_col='year-week'):
    labels = list(df[label_col].unique())
    subsets = []
    for l in labels:
        dates = [0] * 52
        subset = df[df[label_col] == l]
        for idx,row in subset.iterrows():
            (year, week) = row['year-week'].split('-')
            dates[int(week)-1] = row['count']
        print(dates)
        anomalies = isolation_forest(pd.DataFrame(dates,columns=['year-week']))
        
        subsets.append( pd.DataFrame({'weeks':list(range(0,52)),'genomic_address':[l]*52,'anomalies':list(anomalies),'counts':dates }) )
    return pd.concat(subsets)

filename = 'out_line_list.tsv'
x_column = 'date'
y_column = 'genomic_address_name'
df = load_data(filename,date_col=x_column,label_col=y_column)
print(process(df,label_col='genomic_address_name',min_date=None,max_date=None,date_col='year-week').to_csv('anomaly.txt',sep="\t",header=True))

