import pandas as pd
import os
from outbreak_detector.constants import needed_cols_ll, needed_cols_config
from outbreak_detector.utils import calc_md5

class detector:
    status = True
    messages = []
    input_samples = []
    selected_samples = []
    ll_df = None
    outbreak_df = None

    def __init__(self,config) -> None:
        self.needed_cols_ll = needed_cols_ll
        self.needed_cols_config = needed_cols_config

        self.validate_keys(self.needed_cols_config, list(config.keys()))
        if not self.status:
            return
        
        self.rule_key_columns = config['rule_key_columns']
        rpath = config['outbreak_rules_path']
        self.gas_denovo_thresholds = config['gas_denovo_thresholds']
        self.rules = self.process_rules(rpath,self.rule_key_columns)
        if not self.status:
            return
        self.selected_samples = []
        fpath = config['line_list_path']
        df = self.format_df(fpath,col_map=config["column_map"],filters=config['filters'],source_col='source_type')
        df.to_csv("temp.tsv",sep="\t",header=True, index=False)
        self.validate_keys(self.needed_cols_ll, list(df.columns))
        if not self.status:
            return
        
        self.input_samples = list(df['sample_id'])
        outbreak_codes = self.process(df)
        self.outbreak_df = pd.DataFrame.from_dict(outbreak_codes,orient='index')
        self.ll_df = df[df['sample_id'].isin(self.selected_samples)]

    def validate_keys(self, fields, data_keys):
        missing = set(fields) - set(data_keys)
        if len(missing) == 0:
            return
        missing = ",".join(sorted(list(missing)))
        self.messages.append(f'Error: missing needed field(s) {missing}')
        self.status = False

    def get_match_threshold_idx(self,t,thresholds):
        for idx,value in enumerate(thresholds):
            if value == t:
                return idx
        return 0
    
    def process_rules(self,fpath,columns):
        if not self.file_valid(fpath):
            self.status = False
            self.messages.append(f'Error: rule file {fpath} does not exist or is inaccessible')
            return {}
        df = pd.read_csv(fpath,header=0,sep="\t")
        rules = {}
        for idx,row in df.iterrows():
            key = []
            for col in columns:
                v = f'{row[col]}'
                if v == 'nan':
                    v = ''
                key.append(v)
            key = "___".join(key)
            min_total_isolates = row['min_total_isolates']
            min_human_isolates = row['min_human_isolates']
            max_days = row['max_date_delta']
            max_pairwise_diff = row['max_pairwise_threshold']
            rules[key] = {
                'min_total_isolates':min_total_isolates,
                'min_human_isolates':min_human_isolates,
                'max_date_delta': max_days,
                'max_pairwise_threshold': max_pairwise_diff
            }
        return rules

    def calc_date_delta(self,df,date_col='date'):
        dates = df[date_col].tolist()
        date_delta = [0]*len(dates)
        if len(dates) < 2:
            return date_delta
        for i in range(0,len(dates)-1):
            d1 = dates[i]
            d2 = dates[i+1]
            date_delta[i] = int((d2 - d1).days)
        return date_delta

    def format_df(self,fpath,col_map,filters,source_col):
        if not self.file_valid(fpath):
            self.status = False
            self.messages.append(f'Error metadata input {fpath} could not be found or inaccessible')
            return pd.DataFrame()
        df = pd.read_csv(fpath,header=0,sep="\t")
        df = df.rename(columns=col_map)
        cols = set(df.columns)

        num_records = len(df)
        for col in self.needed_cols_ll:
            if col not in cols:
                df[col] = ['']*num_records
        df = df[df['date'].notna()]
        
        df = df[df['gas_denovo_cluster_address'].notna()]
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = self.filter_df(df,filters)
        df = df.sort_values(by=['taxon_name','genomic_address_name','date'], ascending=True)
        df = self.add_taxonomy(df,taxon_col='taxon_name')

        df['denovo_cluster_code'] = self.extract_clusters(df,col_name='gas_denovo_cluster_address',delim='.')
        df['is_human'] = self.detect_human(df,col_name=source_col)
        df = df.sort_values(by=['denovo_cluster_code','date'])
        df['date_delta'] = self.calc_date_delta(df,date_col='date')

        return df.reset_index(drop=True)

    def add_taxonomy(self,df,taxon_col):
        genus = []
        species = []
        subspecies = []
        taxa = df[taxon_col].tolist()
        for taxon in taxa:
            taxon = taxon.split(' ')
            if len(taxon) >= 1:
                genus.append(taxon[0])
            else:
                genus.append('')
            if len(taxon) >= 1:
                species.append(taxon[1])
            else:
                species.append('')
            subsp = ''
            for ssp in ['ssp', 'subsp','subspecies']:
                if ssp in taxon:
                    for i in range(0,len(taxon)):
                        if taxon[i] == ssp:
                            subsp = taxon[i+1]
                            break
            subspecies.append(subsp)
        df['genus'] = genus
        df['species'] = species
        df['subspecies'] = subspecies
        return df

    def detect_human(self,df,col_name):
        data = df[col_name].tolist()
        assign = []
        for source in data:
            source = f'{source}'.lower()
            status = False
            for label in ['human','male','female','homo','homo sapien','patient']:
                if label in source:
                    status = True
                    break
            assign.append(status)
        return assign

    def summarize_denovo_clusters(self,df):
        total_cluster_counts = dict(df['denovo_cluster_code'].value_counts())
        human_cluster_counts = dict(df[df['is_human']]['denovo_cluster_code'].value_counts())
        unassigned_samples = dict(df[df['outbreak_cluster_code_name'].isna()]['denovo_cluster_code'].value_counts())
        summary = {}
        for cluster_code in total_cluster_counts:
            summary[cluster_code] = {
                'total':total_cluster_counts[cluster_code],
                'human':0,
                'unassigned':0,
                'unassigned_samples':set(),
                'outbreak_codes':set(),
                'samples':set(df['sample_id']),
                'rules':{},
                'status':'PASS'
            }
            if cluster_code in human_cluster_counts:
                summary[cluster_code]['human'] = human_cluster_counts[cluster_code]
            if cluster_code in unassigned_samples:
                summary[cluster_code]['unassigned'] = unassigned_samples[cluster_code]
                summary[cluster_code]['unassigned_samples'] = set(df[df['denovo_cluster_code'] == cluster_code]['sample_id'])
            subset = df[df['outbreak_cluster_code_name'].notnull()]
            if len(subset) > 0:
                summary[cluster_code]['outbreak_codes'] = set(subset['outbreak_cluster_code_name'])
   
        return summary
        
    def extract_clusters(self,df,col_name='gas_denovo_cluster_address',delim='.',t=None):
        cluster_assignments = []
        for idx,row in df.iterrows():
            k = self.get_rule_key_row(row,self.rule_key_columns)
            addr = row[col_name]
            (prefix,addr) = addr.split('|')
            addr = addr.split(delim)
            if t is None:
                t = self.rules[k]['max_pairwise_threshold']
            thresh_idx = self.get_match_threshold_idx(t,self.gas_denovo_thresholds)
            if thresh_idx > 0:
                addr = f'{delim}'.join(addr[0:thresh_idx])
                cluster_assignments.append(f'{prefix}|{addr}')
            else:
                cluster_assignments.append(f'{prefix}|{addr[0]}')

        return cluster_assignments
        

    def filter_df(self, df, filters):
        columns = list(df.columns)
        for col in filters:
            if col not in columns:
                continue
            filt = filters[col]
            filt_type = filt[0]
            values = filt[1]
            if filt_type == 'list':
                df = self.filter_by_list(df,col,values)
            elif filt_type == 'range':
                df = self.filter_by_value_range(df,col,values['min'],values['max'])
        return df

    def filter_by_value_range(self, df,colname,min_val,max_val):
        df = df[df[colname] >= min_val]
        return df[df[colname] <= max_val]

    def filter_by_list(self, df,colname,values):
        return df[df[colname].isin(values)]

    def file_valid(self,f):
        if os.path.exists(f) and os.path.getsize(f) > 0:
            return True
        return False

    def get_line_count(self,f):
        return int(os.popen(f'wc -l {f}').read().split()[0])
    
    def get_rule_key_row(self,row,columns):
        rule_key = []
        for col in columns:
            v = f'{row[col]}'
            if v == 'nan':
                v = ''
            rule_key.append(f'{v}')


        num_cols = len(rule_key)
        for i in reversed(range(1,num_cols)):
            rule_key[i] = ''
            k = "___".join(rule_key)
            if k in self.rules:
                return k

        return ''

    def get_rule_key(self,df,columns):
        summaries = {}
        for col in columns:
            summaries[col] = {k: v for k, v in sorted(dict(df[col].value_counts()).items(), key=lambda item: item[1])}

        #Get consensus rule key
        rule_key = []
        for col in columns:
            v = ''
            if len(summaries[col]) > 0:
                v = list(summaries[col].keys())[0]
            rule_key.append(f'{v}')

        num_cols = len(rule_key)
        for i in reversed(range(1,num_cols)):
            rule_key[i] = ''
            k = "___".join(rule_key)
            if k in self.rules:
                return k

        return ''

    def cluster_dates(self,df,max_date_delta):
        date_clusters = [[]]
        clust_id = 0
        sample_ids = list(df['sample_id'])
        date_delta = list(df['date_delta'])

        for idx in range(0,len(sample_ids)-1):
            sample_id = sample_ids[idx]
            if date_delta[idx] > max_date_delta:
                clust_id+=1
                date_clusters.append([])
            date_clusters[clust_id] += [sample_id, sample_ids[idx+1]]

        return date_clusters

    def duplicate_detect(self,df):
        max_delta = 90
        match_columns = ["country","state_province","sex","age"]
        num_cols = len(match_columns)
        df['duplicate_group_code'] = self.extract_clusters(df,col_name='gas_denovo_cluster_address',delim='.',t=1)

        candidates = {}
        duplicate_group_codes = list(set(df['duplicate_group_code']))
        for code in duplicate_group_codes:
            subset = df[df['duplicate_group_code'] == code]
            if len(subset) < 2:
                continue
            for idx,row in subset.iterrows():
                sample_id = row['sample_id']
                meta = [code]
                for col in match_columns:
                    if col in row:
                        meta.append(row[col])
                    else:
                        meta.append('')
                md5 = calc_md5([''.join([str(x) for x in meta])])[0]
                if md5 not in candidates:
                    candidates[md5] = []
                meta.append(sample_id)
                candidates[md5].append(meta)

        candidate_hash_keys = list(candidates.keys())
        for md5 in candidate_hash_keys:
            if len(candidates[md5]) < 2:
                del(candidates[md5])
        return candidates

                                

    def process(self,df):
        denovo_clust_summary = self.summarize_denovo_clusters(df)
        outbreak_clusters = {}
        tracker = 1
        duplicate_candidates = {}
        for cluster_id in denovo_clust_summary:
            subset = df[df['denovo_cluster_code'] == cluster_id]
            rule_key = self.get_rule_key(subset,self.rule_key_columns)
            rule_params = {}
            if rule_key in self.rules:
                rule_params = self.rules[rule_key]
            denovo_clust_summary[cluster_id]['rules'] = rule_params
            if len(rule_params) == 0:
                denovo_clust_summary[cluster_id]['status'] = 'FAIL: Could not find matching rule set'
                continue
            if denovo_clust_summary[cluster_id]['total'] < rule_params['min_total_isolates']:
                denovo_clust_summary[cluster_id]['status'] = 'FAIL: Does not meet minimum number of total isolates for cluster definition'
                continue
            if denovo_clust_summary[cluster_id]['human'] < rule_params['min_human_isolates']:
                denovo_clust_summary[cluster_id]['status'] = 'FAIL: Does not meet minimum number of human isolates for cluster definition'
                continue
            if denovo_clust_summary[cluster_id]['unassigned'] == 0:
                denovo_clust_summary[cluster_id]['status'] = 'FAIL: No new samples to be assigned to an outbreak'
                continue                
            date_clusters = self.cluster_dates(subset, rule_params['max_date_delta'])
            if len(date_clusters) == 0:
                continue
            for i in range(0,len(date_clusters)):
                sample_ids = date_clusters[i]
                if len(sample_ids) < rule_params['min_total_isolates']:
                    continue
                date_df = subset[subset['sample_id'].isin(sample_ids) ]
                duplicate_candidates.update(self.duplicate_detect(date_df.copy()))
                existing_outbreak_codes = set(date_df['outbreak_cluster_code_name'].dropna())
                unassigned_ids = set(sample_ids) & set(denovo_clust_summary[cluster_id]['unassigned_samples'])
                year = list(date_df['date'])[0].year
                year_code = f'{year}'[-2:]
                count_human = len(date_df[date_df['is_human']])
                if count_human < rule_params['min_human_isolates']:
                    continue
                outbreak_code = f'{year_code}_{cluster_id}_{tracker}'
                tracker+=1
                self.selected_samples += sample_ids
                outbreak_clusters[outbreak_code] = {
                    'year':year,
                    'cluster_id':cluster_id,
                    'total_isolates': len(date_df),
                    'human_isolates':count_human,
                    'unassigned_isolates':len(unassigned_ids),
                    'sample_ids': ','.join(sample_ids),
                    'unassigned_samples':','.join(unassigned_ids),
                    'existing_outbreak_codes': existing_outbreak_codes
                }
        self.duplicate_candidates = duplicate_candidates
        return outbreak_clusters
