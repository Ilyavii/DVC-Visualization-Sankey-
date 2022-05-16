import yaml
import pandas as pd
import config


def df_from_dict(pipelines):
	df_pipelines = pd.DataFrame(columns=['key', 'label', 'stage'])
	for stage_name, stage  in pipelines['stages'].items():
		if (stage_name not in config.ignor_stages):
			for key, value in stage.items():
				if (type(value) == type([])):
					for value_from_list in value:
						df_pipelines = df_pipelines.append({'key': key, 'label': value_from_list, 'stage': stage_name}, ignore_index=True) 
				else:
					df_pipelines = df_pipelines.append({'key': key, 'label': value, 'stage': stage_name}, ignore_index=True)  
			
	return df_pipelines

def finde_stage(df, value, from_num, key, stage):
	try:
		if (key == 'cmd' or value.split('.')[-1] in config.ignor_files):
			return "script" # stage
		if (from_num == 0 and key == 'deps'):
			return df[df['outs'] == value]['stage'].tolist()[0]
		elif (key == 'outs'):
			return stage
		else:
			return from_num
	except:
		return from_num


def get_from_number(df_files_input, dict_label):
	if (df_files_input['key'] == 'outs'):
		return dict_label[df_files_input['stage']]
	return dict_label[df_files_input['label']]

def get_to_number(df_files_input, dict_label):
	if (df_files_input['key'] == 'outs'):
		return dict_label[df_files_input['label']]
	return dict_label[df_files_input['stage']]



def find_name_file(path_list):
	for i, path in enumerate(path_list):
		path_list[i] = path.split('/')[-1]
	return path_list



def df_split_into_columns(df_pipelines):
	df_pipelines_cmd = df_pipelines[df_pipelines['key'] == 'cmd']
	df_pipelines_cmd = df_pipelines_cmd.rename(columns={'label':'cmd'})
	del df_pipelines_cmd["key"]
	df_pipelines_deps = df_pipelines[df_pipelines['key'] == 'deps']
	df_pipelines_deps = df_pipelines_deps.rename(columns={'label':'deps'})
	del df_pipelines_deps["key"]
	df_pipelines_outs = df_pipelines[df_pipelines['key'] == 'outs']
	df_pipelines_outs = df_pipelines_outs.rename(columns={'label':'outs'})
	del df_pipelines_outs["key"]

	df = df_pipelines_cmd.merge(df_pipelines_deps, how='outer', on='stage')
	df = df.merge(df_pipelines_outs, how='outer', on='stage')
	return df