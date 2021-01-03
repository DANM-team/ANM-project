from graphviz import Digraph
def plot_trace(trace):
    u = Digraph('unix', format='png', strict=True)
    names = trace[['id', 'cmdb_id', 'service_name', 'ds_name']].set_index('id')
    for index, row in trace.iterrows():
        child_id = row['id']
        parent_id = row['pid']

        parent = names.loc[parent_id][['cmdb_id', 'service_name', 'ds_name']] if parent_id in names.index else {'cmdb_id': parent_id, 'service_name': parent_id}
        child = names.loc[child_id][['cmdb_id', 'service_name', 'ds_name']]
        
        if row['call_type'] in ['JDBC']:
            u.edge(str(parent['cmdb_id']), str(child['ds_name']), label=row['call_type'])
        # elif row['call_type'] in ['CSF']:
        #     u.edge(str(parent['cmdb_id']), str(child['service_name']), label=row['call_type'])
        elif row['call_type'] in ['FlyRemote', 'OSB', 'CSF']:
            # skip these call types
            continue
        else:
            u.edge(str(parent['cmdb_id']), str(child['cmdb_id']), label=row['call_type'])
        
    return u