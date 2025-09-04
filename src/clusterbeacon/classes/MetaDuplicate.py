from src.clusterbeacon.utils import calc_md5
class MetaDuplicate:
    def __init__(self):
        pass
    
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